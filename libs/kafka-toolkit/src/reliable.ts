import { EventEnvelope, parseEnvelope } from '@poc/contracts';
import { Idempotency } from './idempotency';
import { LoggerLike, sleep } from './mongo';
import { EventPublisher, RawMessage } from './publisher';

/**
 * Contexto minimo que necesitamos del consumidor. Es estructural: cualquier
 * KafkaContext de @nestjs/microservices lo satisface sin importar Nest aca.
 */
export interface ConsumerContext {
  getTopic(): string;
  getMessage(): RawKafkaMessage;
  getConsumer(): {
    commitOffsets(
      offsets: Array<{ topic: string; partition: number; offset: string }>,
    ): Promise<void>;
  };
}

export type RawKafkaMessage = RawMessage & { partition: number; offset: string };

export interface ReliableConsumerDeps {
  idempotency: Idempotency;
  publisher: EventPublisher;
  logger?: LoggerLike;
  /** Intentos antes de mandar a la DLQ (default 3, RNF-04). */
  maxAttempts?: number;
  /** Espera base de los reintentos: 1 s, 2 s, 4 s (Decisiones tecnicas #2). */
  baseDelayMs?: number;
}

/**
 * Envuelve el trabajo de cada @EventPattern con las tres garantias:
 *
 *  1. validacion del sobre -> si es invalido, va directo a la DLQ;
 *  2. reintentos con espera creciente y despues DLQ (el mensaje roto no
 *     bloquea la particion);
 *  3. idempotencia (transaccion con processed_events) y recien ahi
 *     commit MANUAL del offset: nunca antes de procesar.
 *
 * Este metodo NUNCA lanza excepciones: si algo sale muy mal igual se
 * confirma el offset (prevalece no bloquear la particion) y se loguea.
 */
export class ReliableConsumer {
  private readonly idempotency: Idempotency;
  private readonly publisher: EventPublisher;
  private readonly logger: LoggerLike;
  private readonly maxAttempts: number;
  private readonly baseDelayMs: number;

  constructor(deps: ReliableConsumerDeps) {
    this.idempotency = deps.idempotency;
    this.publisher = deps.publisher;
    this.logger = deps.logger ?? console;
    this.maxAttempts = deps.maxAttempts ?? 3;
    this.baseDelayMs = deps.baseDelayMs ?? 1000;
  }

  async handle<T extends EventEnvelope = EventEnvelope>(
    ctx: ConsumerContext,
    raw: unknown,
    work: (event: T) => Promise<void>,
  ): Promise<void> {
    const topic = ctx.getTopic();
    const message = ctx.getMessage();

    let event: T;
    try {
      event = parseEnvelope(raw) as T;
    } catch (err) {
      this.logger.error(`[kafka] ${topic} mensaje invalido -> DLQ: ${(err as Error).message}`);
      await this.deadLetter(topic, message, err);
      await this.commit(ctx, topic, message);
      return;
    }

    for (let attempt = 1; attempt <= this.maxAttempts; attempt++) {
      try {
        const result = await this.idempotency.run(event.eventId, () => work(event));
        if (result === 'skipped') {
          this.logger.log(`[kafka] ${topic} evento repetido ${event.eventId} ignorado (idempotencia)`);
        }
        await this.commit(ctx, topic, message);
        return;
      } catch (err) {
        const reason = err instanceof Error ? err.message : String(err);
        this.logger.error(
          `[kafka] ${topic} intento ${attempt}/${this.maxAttempts} fallo para ${event.eventId}: ${reason}`,
        );
        if (attempt < this.maxAttempts) {
          await sleep(this.baseDelayMs * 2 ** (attempt - 1)); // 1 s, 2 s, 4 s
          continue;
        }
        this.logger.error(`[kafka] ${topic} ${event.eventId} agoto reintentos -> DLQ`);
        await this.deadLetter(topic, message, err);
        await this.commit(ctx, topic, message);
        return;
      }
    }
  }

  /** Commit manual del offset, SIEMPRE despues de procesar (o de enviar a DLQ). */
  private async commit(ctx: ConsumerContext, topic: string, message: RawKafkaMessage): Promise<void> {
    try {
      const nextOffset = (BigInt(message.offset) + 1n).toString();
      await ctx.getConsumer().commitOffsets([
        { topic, partition: message.partition, offset: nextOffset },
      ]);
    } catch (err) {
      this.logger.error(
        `[kafka] commit falló en ${topic}[${message.partition}]@${message.offset}: ${
          (err as Error).message
        } (se reprocesara en el proximo arranque)`,
      );
    }
  }

  private async deadLetter(topic: string, message: RawKafkaMessage, error: unknown): Promise<void> {
    for (let attempt = 1; attempt <= 3; attempt++) {
      try {
        await this.publisher.publishDlq(topic, message, error, this.maxAttempts);
        return;
      } catch (err) {
        this.logger.error(
          `[kafka] publicacion a DLQ falla (intento ${attempt}/3): ${(err as Error).message}`,
        );
        await sleep(250);
      }
    }
    // No lanzamos: aunque la DLQ este caida, la particion no puede quedar bloqueada.
    this.logger.error(`[kafka] ${topic} mensaje NO pudo ir a la DLQ; se confirma igual el offset`);
  }
}
