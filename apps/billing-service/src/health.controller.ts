import { Controller, Get } from '@nestjs/common';
import { BillingService } from './billing.service';

@Controller()
export class HealthController {
  constructor(private readonly billing: BillingService) {}

  @Get('health')
  health(): { ok: boolean; service: string; at: string } {
    return { ok: true, service: 'billing-service', at: new Date().toISOString() };
  }

  /** Vista de ayuda para la demo: cuentas simuladas y su estado. */
  @Get('accounts')
  async accounts(): Promise<unknown> {
    return this.billing.listAccounts();
  }
}
