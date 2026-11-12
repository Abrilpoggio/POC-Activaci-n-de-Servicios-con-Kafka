import { Controller, Get } from '@nestjs/common';
import { LoyaltyService } from './loyalty.service';

@Controller()
export class HealthController {
  constructor(private readonly loyalty: LoyaltyService) {}

  @Get('health')
  health(): { ok: boolean; service: string; at: string } {
    return { ok: true, service: 'loyalty-service', at: new Date().toISOString() };
  }

  /** Saldos de puntos (Escenario 5), de mayor a menor. */
  @Get('points')
  async points(): Promise<unknown> {
    return this.loyalty.listPoints();
  }
}
