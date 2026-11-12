import { Controller, Get } from '@nestjs/common';
import { NotificationService } from './notification.service';

@Controller()
export class HealthController {
  constructor(private readonly notification: NotificationService) {}

  @Get('health')
  health(): { ok: boolean; service: string; at: string } {
    return { ok: true, service: 'notification-service', at: new Date().toISOString() };
  }

  /** Ultimos 20 notificaciones enviadas, para la demo. */
  @Get('notifications')
  async notifications(): Promise<unknown> {
    return this.notification.listNotifications();
  }
}
