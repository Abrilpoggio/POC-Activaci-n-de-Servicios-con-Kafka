import { Controller, Get } from '@nestjs/common';
import { ProvisioningService } from './provisioning.service';

@Controller()
export class HealthController {
  constructor(private readonly provisioning: ProvisioningService) {}

  @Get('health')
  health(): { ok: boolean; service: string; at: string } {
    return { ok: true, service: 'provisioning-service', at: new Date().toISOString() };
  }

  /** Ultimas 50 ordenes de aprovisionamiento, para la demo. */
  @Get('orders')
  async orders(): Promise<unknown> {
    return this.provisioning.listOrders();
  }
}
