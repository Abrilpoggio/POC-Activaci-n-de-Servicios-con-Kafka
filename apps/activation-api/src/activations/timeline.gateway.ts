import { Injectable, Logger } from '@nestjs/common';
import { OnGatewayConnection, OnGatewayInit, WebSocketGateway, WebSocketServer } from '@nestjs/websockets';
import { Server, Socket } from 'socket.io';

export interface TimelineEvent {
  activationId: string;
  correlationId: string;
  eventType: string;
  occurredAt: string;
  source: string;
  status: string;
  payload: Record<string, unknown>;
}

/**
 * RF-08: envia cada cambio de la saga a la Demo UI en tiempo real (WebSocket).
 */
@Injectable()
@WebSocketGateway({ cors: { origin: '*' } })
export class TimelineGateway implements OnGatewayInit, OnGatewayConnection {
  @WebSocketServer()
  server!: Server;

  private readonly logger = new Logger(TimelineGateway.name);

  afterInit(): void {
    this.logger.log('WebSocket de timeline listo (socket.io)');
  }

  handleConnection(client: Socket): void {
    this.logger.debug(`UI conectada: ${client.id}`);
  }

  broadcast(event: TimelineEvent): void {
    if (this.server) {
      this.server.emit('timeline', event);
    }
  }
}
