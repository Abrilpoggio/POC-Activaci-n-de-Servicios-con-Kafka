import { Inject, Injectable, OnModuleInit } from '@nestjs/common';
import { EventEnvelope } from '@poc/contracts';
import { MongoHandle } from '@poc/kafka-toolkit';
import { Collection, Db } from 'mongodb';
import * as nodemailer from 'nodemailer';

export interface NotificationDoc {
  eventId: string;
  activationId: string;
  eventType: string;
  to: string;
  subject: string;
  sentAt: string;
}

// SMTP local de la demo (por defecto MailHog/Mailpit en el puerto 1025).
const transporter = nodemailer.createTransport({
  host: process.env.SMTP_HOST ?? 'localhost',
  port: Number(process.env.SMTP_PORT ?? 1025),
  ignoreTLS: true,
});

/**
 * notification-service:
 *  - consume ActivationCompleted / ActivationFailed y envia un mail;
 *  - registra cada envio en `notifications` dentro de la transaccion de
 *    idempotencia: si sendMail falla, se revierte y el reintento reenvia;
 *    si el evento esta repetido, processed_events lo descarta y NO se reenvia.
 */
@Injectable()
export class NotificationService implements OnModuleInit {
  constructor(@Inject('MONGO_HANDLE') private readonly mongo: MongoHandle) {}

  private get db(): Db {
    return this.mongo.db;
  }

  private get notifications(): Collection<NotificationDoc> {
    return this.db.collection<NotificationDoc>('notifications');
  }

  /** Una notificacion por activacion y tipo de evento. */
  async onModuleInit(): Promise<void> {
    await this.notifications.createIndex({ activationId: 1, eventType: 1 });
  }

  /** RF-07: envia el mail de resultado de la activacion. */
  async onActivationOutcome(event: EventEnvelope<'ActivationCompleted' | 'ActivationFailed'>): Promise<void> {
    const to = `cliente-${event.customerId}@demo.local`;
    let subject: string;
    let html: string;

    if (event.eventType === 'ActivationCompleted') {
      const { planId } = event.payload as { planId: string };
      subject = `[POC] Activación ${event.correlationId} activada`;
      html = `
        <h1>¡Activación completada!</h1>
        <p>Hola, tu activación <strong>${event.correlationId}</strong> finalizó correctamente.</p>
        <p>Plan: <strong>${planId}</strong></p>
        <p>¡Bienvenido/a a la POC!</p>`;
    } else if (event.eventType === 'ActivationFailed') {
      const { failedStep, reason } = event.payload as { failedStep: string; reason: string };
      subject = `[POC] Activación ${event.correlationId} falló`;
      html = `
        <h1>Activación fallida</h1>
        <p>La activación <strong>${event.correlationId}</strong> no pudo completarse.</p>
        <p>Paso: <strong>${failedStep}</strong></p>
        <p>Motivo: ${reason}</p>`;
    } else {
      // Otro tipo de evento: solo queda marcado como procesado, no se envía nada.
      return;
    }

    const session = this.mongo.client.startSession();
    try {
      await session.withTransaction(async () => {
        await this.notifications.insertOne(
          {
            eventId: event.eventId,
            activationId: event.correlationId,
            eventType: event.eventType,
            to,
            subject,
            sentAt: new Date().toISOString(),
          },
          { session },
        );

        // Si sendMail falla, la transacción se revierte y el reintento reenvía.
        await transporter.sendMail({ from: 'activaciones@poc.local', to, subject, html });
      });
    } finally {
      await session.endSession();
    }
  }

  /** Ultimos 20 envios, para la demo. */
  async listNotifications(): Promise<NotificationDoc[]> {
    return this.notifications.find({}).sort({ sentAt: -1 }).limit(20).toArray();
  }
}
