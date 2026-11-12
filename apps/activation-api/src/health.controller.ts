import { Controller, Get } from '@nestjs/common';

@Controller()
export class HealthController {
  @Get('health')
  health(): { ok: boolean; service: string; at: string } {
    return { ok: true, service: 'activation-api', at: new Date().toISOString() };
  }
}
