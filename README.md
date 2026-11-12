# POC · Activación de Servicios con Kafka

Sistemas Distribuidos · 2026 · Ingeniería en Sistemas de Información

La activación de un plan se resuelve publicando **un único evento en Kafka**: ningún
servicio llama directamente a otro. Un cliente contrata un plan desde una UI en React;
eso dispara facturación, aprovisionamiento, notificación y registro en CRM,
coordinados por eventos (coreografía con agregador).

## Quickstart

```bash
docker compose up -d --build
```

| URL | Qué ver |
|---|---|
| http://localhost:5173 | Demo UI (formulario de contratación + timeline en vivo) |
| http://localhost:8080 | Kafka UI (topics, mensajes, particiones, consumer groups, lag) |
| http://localhost:8025 | MailHog (emails simulados) |
| http://localhost:3000/activations | API REST de activaciones |
| http://localhost:9092 | Kafka (broker KRaft, solo para clientes de la host) |

Escenario 5 (consumidor nuevo con replay del log):

```bash
docker compose --profile loyalty up -d loyalty-service
```

## Requerimientos → dónde están implementados

| ID | Dónde |
|---|---|
| RF-01/02/03 | `apps/activation-api` · `POST /activations` responde 202 y encola `ActivationRequested` por outbox |
| RF-04 | `billing-service` y `provisioning-service` consumen el mismo evento en paralelo y publican su resultado |
| RF-05/06 | `activations.service.ts` · `applyResult()` agrega resultados; primer fallo → `FAILED` + `ActivationFailed`; billing compensa (`saga_failures` + `BillingAccountCancelled`) |
| RF-07 | `notification-service` · un email por `ActivationCompleted`/`ActivationFailed` vía MailHog |
| RF-08 | `timeline.gateway.ts` (socket.io) → Demo UI |
| RF-09 | `GET /activations/:id` devuelve estado e historial |
| RF-10 | `crm-analytics-service` registra todo; `POST /event-log/replay` reprocesa desde el inicio con un group nuevo |
| RNF-01 | `docker-compose.yml` (un solo comando) |
| RNF-02 | Clave de mensaje = `customerId` (`EventPublisher.publish()`) |
| RNF-03 | `Idempotency.run()` · `processed_events` (índice único) en la misma transacción que el efecto |
| RNF-04 | `ReliableConsumer.handle()` · 3 intentos (1 s, 2 s, 4 s) → `<topic>.dlq` con headers del error |
| RNF-05 | Commit manual del offset **después** de procesar; offsets por consumer group |
| RNF-06 | `correlationId` en todos los eventos (= `activationId`) |
| RNF-07 | `libs/contracts` · sobre común versionado (`EVENT_VERSIONS`) |

## Estructura

```
activation-poc/
├── apps/
│   ├── activation-api/          REST + WebSocket + agregador de la saga
│   ├── billing-service/         cuentas + compensación (outbox)
│   ├── provisioning-service/    aprovisionamiento simulado (outbox)
│   ├── notification-service/    emails vía MailHog
│   ├── crm-analytics-service/   log de eventos + replay
│   ├── loyalty-service/         perfil "loyalty": replay del log (escenario 5)
│   └── demo-ui/                 React + Vite + socket.io-client
├── libs/
│   ├── contracts/               única fuente de verdad de los eventos
│   └── kafka-toolkit/           idempotencia, reintentos, DLQ, outbox
├── docker/                      create-topics.sh, Dockerfiles, nginx.conf
├── docker-compose.yml           infra + servicios + UI
└── docs/
    ├── architecture.md          arquitectura completa y mapeo de requerimientos
    └── demo-script.md           guion de demo (5 escenarios, 15 min)
```

## Temas de Kafka que muestra la demo

- **Fan-out**: un evento, varias reacciones (`activation.requested` → billing + provisioning + CRM).
- **Saga sin llamadas directas**: compensación reactiva cuando algo falla.
- **Offsets y consumer groups**: cae un servicio, no se pierde un mensaje, al volver procesa el lag.
- **Consumer groups y particiones**: `--scale billing-service=2` reparte 3 particiones entre 2 consumidores.
- **Replay del log**: un consumidor nuevo (`loyalty-svc`, `auto.offset.reset=earliest`) procesa todo el historial sin tocar a los demás.
- **Idempotencia + DLQ**: eventos repetidos no duplican efectos; mensajes inválidos van a la DLQ sin frenar la partición.

## Desarrollo local (sin Docker para las apps)

```bash
npm install
npm run build            # libs + servicios
npm run start:api        # y/o start:billing, start:provisioning, ...
```

Requiere Kafka (`localhost:9092`) y MongoDB en replica set (`localhost:27017`).

## Comandos de la demo

```bash
docker compose stop notification-service     # escenario 3: servicio caído
docker compose start notification-service
docker compose up -d --scale billing-service=2   # escenario 4: escalar
docker compose --profile loyalty up -d loyalty-service  # escenario 5: replay
curl -X POST http://localhost:3000/dev/poison          # extra: mensaje a la DLQ
```
