import { dlqTopic, EventEnvelope } from '@poc/contracts';
import { Kafka, logLevel, Producer } from 'kafkajs';
import { LoggerLike } from './mongo';

export interface PublisherOptions {
  brokers: string[];
  clientId: string;
  logger?: LoggerLike;
}

export interface RawMessage {
  /** En el contexto de Nest puede venir ya decodificado (string u objeto). */
  key: Buffer | string | null;
  value: Buffer | string | object | null;
  headers?: Record<string, Buffer | string | Array<Buffer | string> | undefined>;
}

/**
 * Cliente de publicacion (unico punto de salida hacia Kafka).
 * Se dejo aislado aca para poder reemplazarlo (kafkajs -> cliente oficial
 * de Confluent) sin tocar los servicios: ver "Decisiones tecnicas".
 *
 * La clave del mensaje es siempre customerId (RNF-02: orden por cliente).
 */
export class EventPublisher {
  private readonly producer: Producer;
  private readonly logger: LoggerLike;

  constructor(private readonly options: PublisherOptions) {
    this.logger = options.logger ?? console;
    const kafka = new Kafka({
      clientId: options.clientId,
      brokers: options.brokers,
      logLevel: logLevel.NOTHING,
      retry: { retries: 8 },
    });
    this.producer = kafka.producer({ allowAutoTopicCreation: false, idempotent: true });
  }

  async connect(): Promise<void> {
    await this.producer.connect();
  }

  async disconnect(): Promise<void> {
    await this.producer.disconnect().catch(() => undefined);
  }

  /** Publica un sobre en el topic dado, con clave = customerId. */
  async publish(topic: string, event: EventEnvelope): Promise<void> {
    // Headers tolerantes a sobres invalidos (utilidades de demo como /dev/poison):
    // kafkajs no acepta valores undefined.
    const headers: Record<string, string> = {};
    const add = (name: string, value: unknown): void => {
      if (value !== undefined && value !== null) headers[name] = String(value);
    };
    add('eventId', event.eventId);
    add('eventType', event.eventType);
    add('correlationId', event.correlationId);
    add('customerId', event.customerId);
    add('source', event.source);
    add('eventVersion', event.version);

    await this.producer.send({
      topic,
      acks: -1,
      messages: [
        {
          key: Buffer.from(event.customerId ?? ''),
          value: Buffer.from(JSON.stringify(event)),
          headers,
        },
      ],
    });
  }

  /**
   * Publica el mensaje original en <topic>.dlq con el motivo del error en los
   * headers (RNF-04). Se publica tal cual llego, para poder inspeccionarlo en Kafka UI.
   */
  async publishDlq(originalTopic: string, message: RawMessage, error: unknown, attempts: number): Promise<void> {
    const reason = error instanceof Error ? error.message : String(error);
    // El KafkaContext de Nest entrega value ya decodificado (objeto JS) y key
    // como string; kafkajs necesita Buffers para publicar. Los normalizamos.
    const toBuffer = (v: unknown): Buffer | null => {
      if (v === null || v === undefined) return null;
      if (Buffer.isBuffer(v)) return v;
      if (typeof v === 'string') return Buffer.from(v);
      return Buffer.from(JSON.stringify(v));
    };
    // Normaliza los headers que llegan del consumidor (pueden venir como
    // Buffer, string u objeto) a valores que kafkajs acepte.
    const headers: Record<string, Buffer | Buffer[]> = {};
    for (const [name, value] of Object.entries(message.headers ?? {})) {
      if (value === undefined || value === null) continue;
      if (Buffer.isBuffer(value)) {
        headers[name] = value;
      } else if (Array.isArray(value)) {
        headers[name] = value.map((v) => toBuffer(v) ?? Buffer.alloc(0));
      } else if (typeof value === 'string') {
        headers[name] = Buffer.from(value);
      } else {
        headers[name] = Buffer.from(JSON.stringify(value));
      }
    }
    await this.producer.send({
      topic: dlqTopic(originalTopic),
      messages: [
        {
          key: toBuffer(message.key),
          value: toBuffer(message.value),
          headers: {
            ...headers,
            'x-original-topic': Buffer.from(originalTopic),
            'x-error': Buffer.from(reason.slice(0, 1000)),
            'x-error-at': Buffer.from(new Date().toISOString()),
            'x-attempts': Buffer.from(String(attempts)),
          },
        },
      ],
    });
  }
}
