# Guion de demo — 15 minutos · POC Activación de Servicios con Kafka

Materia: Sistemas Distribuidos 2026 · Compañero de este documento: [`architecture.md`](./architecture.md)

**Pantallas que se usan**

| Pantalla | URL | Para qué |
|---|---|---|
| **Demo UI** | <http://localhost:5173> | Contratar planes y ver el timeline en vivo (socket.io) |
| **Kafka UI** | <http://localhost:8080> | Tópicos, mensajes, consumer groups, lag, offsets, DLQ |
| **MailHog** | <http://localhost:8025> | Emails enviados por `notification-service` (SMTP `localhost:1025`) |

**Cronograma sugerido (15 min)**

| Tiempo | Bloque |
|---|---|
| 0:00–1:30 | Prerrequisitos y arquitectura en 30 segundos |
| 1:30–4:30 | Escenario 1 — camino feliz (fan-out) |
| 4:30–7:30 | Escenario 2 — fallo y compensación |
| 7:30–9:30 | Escenario 3 — servicio caído (lag + retoma) |
| 9:30–11:00 | Escenario 4 — escalar consumidores |
| 11:00–13:00 | Escenario 5 — nuevo consumidor (replay / earliest) |
| 13:00–15:00 | Extras — DLQ (`/dev/poison`) e idempotencia (`/dev/replay`) |

---

## 0. Prerrequisitos

1. Desde la raíz del repo, levantar todo el stack (build incluido):

   ```bash
   docker compose up -d --build
   # o, sin rebuild:
   docker compose up
   ```

   > Si tu archivo de compose vive en `docker/`, usá `docker compose -f docker/docker-compose.yml up -d --build`.

2. Esperar a que esté todo sano (kafka-init crea los tópicos, mongo-init inicializa el replica set `rs0`, los servicios levantan sus consumidores):

   ```bash
   docker compose ps
   curl -s http://localhost:3000/health          # activation-api
   ```

3. Verificar en Kafka UI (<http://localhost:8080>) que los 4 tópicos + sus `.dlq` existen y que los consumer groups aparecen sin lag:

   `activation-api`, `billing-svc`, `provisioning-svc`, `notification-svc`, `crm-analytics`
   (`loyalty-svc` aparece recién en el Escenario 5).

4. Abrir las tres pantallas: Demo UI (:5173), Kafka UI (:8080), MailHog (:8025).

**Guion de apertura (30 s):** “Seis servicios NestJS conversan por Kafka. Nadie llama a nadie por RPC: cada uno reacciona a eventos. La saga la agrega `activation-api`, que también publica el resultado final para que el resto (compensación, email, analítica, fidelización) reaccione.”

---

## Escenario 1 — Camino feliz (fan-out) · ~3 min

**Concepto de Kafka que demuestra:** productor → 2 consumidores en paralelo sobre el mismo mensaje (fan-out por consumer group), y después fan-out de `activation.events` a 4 grupos distintos; orden por clave `customerId`.

**Pasos**

1. En **Demo UI**, completar el formulario: `customerId = C-1234`, plan a elección, *Forzar fallo* = **Ninguno**, y clic en **Contratar plan**.
2. Ver el mensaje `Activación act-… creada (PENDING)` y observar el timeline que se va llenando en vivo.
3. Verificar que aparecen **4 eventos** en el timeline:

   | # | Evento | Source |
   |---|---|---|
   | 1 | `ActivationRequested` | `activation-api` |
   | 2 | `BillingAccountCreated` | `billing-svc` |
   | 3 | `ProvisioningCompleted` | `provisioning-svc` |
   | 4 | `ActivationCompleted` | `activation-api` |

   El badge de estado pasa `Pendiente` → `En curso` → **`Activa`** y aparece el resumen ✅ *Activación completada*. Los chips muestran `billing: OK` y `provisioning: OK`.

4. **Kafka UI** → tópico `activation.requested` → ver el mensaje recién publicado: clave `C-1234`, headers `eventId`, `eventType`, `correlationId`, `customerId`, `source`, `eventVersion`. Repetir la vista en `activation.events`.
5. **Kafka UI** → sección *Consumers* → mostrar que `billing-svc` y `provisioning-svc` consumieron `activation.requested` y que `billing-svc`, `notification-svc`, `crm-analytics` consumieron `activation.events` (**fan-out**: cada grupo tiene su propio offset, todos reciben el mismo evento).
6. **MailHog** → abrir el email de éxito (asunto de activación) y mostrar el cuerpo con el `activationId`.

**Qué se ve:** timeline de 4 eventos en orden, estado `Activa`, 1 email en MailHog, mensajes con clave `customerId` en Kafka UI.

---

## Escenario 2 — Fallo y compensación · ~3 min

**Concepto de Kafka que demuestra:** compensación de saga vía eventos (sin llamadas RPC), y que un fallo de **negocio** se publica como evento normal (no va a la DLQ).

**Pasos**

1. En **Demo UI**, nuevo `customerId = C-2048`, *Forzar fallo* = **En provisioning** → **Contratar plan**.
2. Observar el timeline (5 eventos):

   | Evento | Source | Qué pasó |
   |---|---|---|
   | `ActivationRequested` | `activation-api` | arranca la saga con `simulateFailure: provisioning` |
   | `BillingAccountCreated` | `billing-svc` | billing terminó bien **antes** del fallo |
   | `ProvisioningFailed` | `provisioning-svc` | falla simulada (error de negocio, con `reason`) |
   | `ActivationFailed` | `activation-api` | el agregador detecta el **primer fallo** → `FAILED`, `failedStep: provisioning` |
   | `BillingAccountCancelled` | `billing-svc` | **compensación**: billing reacciona a `ActivationFailed` y anula la cuenta |

3. Ver el badge **`Fallida`** y el resumen ❌ *Activación fallida (compensación)*.
4. Verificar la compensación en el estado de las cuentas:

   ```bash
   # puerto de billing: ver docker compose ps / docker compose port billing-service 3000
   curl -s http://localhost:<puerto-billing>/accounts
   ```

   La cuenta de `C-2048` aparece con `status: "CANCELLED"` y `cancelReason`. Alternativa por Mongo:

   ```bash
   docker compose exec mongo mongosh --quiet \
     --eval 'db.getSiblingDB("billing").billing_accounts.find({customerId:"C-2048"}).toArray()'
   ```

5. **MailHog** → abrir el **email de error** de `C-2048` (evento `ActivationFailed`).

**Qué se ve:** timeline `…ProvisioningFailed → ActivationFailed → BillingAccountCancelled`, cuenta `CANCELLED`, email de error.
**Nota de guion:** como `billing` no falló, la saga igual termina `FAILED`: manda el **primer** fallo.

---

## Escenario 3 — Servicio caído: lag y retoma · ~2 min

**Concepto de Kafka que demuestra:** el log sobrevive a los consumidores; **lag** = mensajes sin consumir; al volver, el grupo retoma desde su último offset y no pierde ni duplica mensajes (commit manual + idempotencia).

**Pasos**

1. Detener el servicio de notificaciones:

   ```bash
   docker compose stop notification-service
   ```

2. Crear **3 activaciones** seguidas en la Demo UI (Forzar fallo = Ninguno). Ver en el timeline que la saga completa (`ACTIVE`), aunque los emails no lleguen.
3. **Kafka UI** → *Consumers* → grupo `notification-svc`: mostrar **lag creciente** (3 mensajes sin consumir) en `activation.events`. Los demás grupos (`billing-svc`, `crm-analytics`, …) siguen con lag 0: cada grupo consume independientemente.
4. Volver a levantar el servicio:

   ```bash
   docker compose start notification-service
   ```

5. **MailHog** → llegarán **los 3 emails** de una. En **Kafka UI**, el lag de `notification-svc` vuelve a 0 y los offsets avanzaron.

**Qué se ve:** lag 3 → 0 en Kafka UI; 3 emails en MailHog; ningún evento perdido ni repetido.

---

## Escenario 4 — Escalar consumidores · ~1,5 min

**Concepto de Kafka que demuestra:** *consumer group* + **particiones**: las instancias de un grupo se reparten las 3 particiones y no compiten entre grupos.

**Pasos**

1. Escalar billing a dos instancias:

   ```bash
   docker compose up -d --scale billing-service=2
   ```

2. **Kafka UI** → *Consumers* → grupo `billing-svc`: ahora hay **2 consumidores** (misma instancia del grupo) repartidos sobre **3 particiones** de `activation.requested` (una partición la toma un consumidor, las otras dos el otro; el rebalanceo es automático).
3. Crear 2 o 3 activaciones nuevas: las procesa indistintamente la instancia 1 o 2 — el resultado en la Demo UI es idéntico.
4. Volver a una instancia:

   ```bash
   docker compose up -d --scale billing-service=1
   ```

**Qué se ve:** 2 consumidores del mismo grupo, reparto de particiones, y que escalar no cambia el comportamiento observable.

---

## Escenario 5 — Nuevo consumidor (replay / desde el principio) · ~2 min

**Concepto de Kafka que demuestra:** agregar un consumidor **nuevo** con grupo propio y `fromBeginning: true` (offset `earliest`) puede relear todo el historial **sin tocar** a los grupos existentes.

**Pasos**

1. Arrancar el servicio de fidelización (perfil `loyalty`):

   ```bash
   docker compose --profile loyalty up -d loyalty-service
   ```

2. En **Kafka UI**, mostrar el grupo nuevo `loyalty-svc` sobre `activation.events` con offset inicial **earliest**: en segundos procesa todo el backlog y el lag baja a 0. Los grupos viejos (`billing-svc`, `notification-svc`, `crm-analytics`) **no se ven afectados**: cada grupo tiene sus propios offsets.
3. Verificar los puntos acumulados:

   ```bash
   curl -s http://localhost:<puerto-loyalty>/points
   ```

4. Mismo concepto con crm-analytics: relanzar su consumidor con un **grupo nuevo desde earliest** (replay de todo `activation.events`):

   ```bash
   curl -s -X POST http://localhost:<puerto-crm>/event-log/replay \
     -H "Content-Type: application/json"
   ```

   Luego ver el historial re-proyectado:

   ```bash
   curl -s http://localhost:<puerto-crm>/stats
   curl -s http://localhost:<puerto-crm>/event-log
   ```

**Qué se ve:** un consumidor nuevo “viaja en el tiempo” leyendo el log completo; los consumidores viejos siguen donde estaban.

---

## Extras (si queda tiempo) · ~2 min

### Extra A — Prueba de DLQ (sobre inválido)

**Concepto:** mensajes que no pueden procesarse no bloquean la partición: van a `<topic>.dlq` con el motivo en headers.

1. Publicar un sobre roto (JSON válido pero sin `eventId`, `correlationId`, `version`, `source`, `occurredAt`):

   ```bash
   curl -s -X POST http://localhost:3000/dev/poison \
     -H "Content-Type: application/json" -d '{}'
   ```

2. **Kafka UI** → tópico `activation.requested.dlq` → aparece el mensaje con los headers:

   - `x-original-topic: activation.requested`
   - `x-error: Evento invalido: falta eventId`
   - `x-error-at: <timestamp>` · `x-attempts: 3`
3. Volver a la Demo UI: crear otra activación y ver que el flujo sigue normal — el mensaje malo **no bloqueó** `activation.requested` (además, el consumidor de `activation.requested` lo descarta igual al validar).

### Extra B — Prueba de idempotencia (mismo `eventId` dos veces)

**Concepto:** *at-least-once* + deduplicación por `eventId` en `processed_events` (índice único en la misma transacción que el efecto) = efecto único.

1. Copiar el **valor completo** de un sobre de Kafka UI (por ejemplo un `BillingAccountCreated` en `billing.events` o un `ActivationRequested` en `activation.requested`), incluido su `eventId`.
2. Reenviarlo con el mismo `eventId`:

   ```bash
   curl -s -X POST http://localhost:3000/dev/replay \
     -H "Content-Type: application/json" \
     -d '{"topic":"billing.events","event":{ ...sobre copiado tal cual... }}'
   ```

3. Verificar que **no se duplica nada**:
   - `GET /accounts` → sigue habiendo **una sola cuenta** para ese `activationId`;
   - **MailHog** → no aparece un email repetido;
   - `GET /activations/:id` → el `history` no suma eventos duplicados;
   - logs del consumidor: `[kafka] billing.events evento repetido <eventId> ignorado (idempotencia)` (ver con `docker compose logs -f billing-service`).

```bash
# comandos de apoyo
docker compose logs -f billing-service | grep -i idempotencia
docker compose logs -f activation-api | grep -E "DLQ|invalido"
curl -s http://localhost:3000/activations | head -c 400
```

---

## Cierre (30 s)

- **4 tópicos** de negocio + sus DLQ, clave `customerId` en todos los mensajes.
- **Garantías**: idempotencia transaccional, reintentos 1 s/2 s/4 s + DLQ con headers, outbox transaccional cada 500 ms, commit manual de offsets.
- **Flexibilidad**: escalar instancias (Escenario 4), servicios caídos sin perder datos (Escenario 3) y consumidores nuevos releyendo el historial (Escenario 5) — todo sin tocar el código de los demás servicios.
