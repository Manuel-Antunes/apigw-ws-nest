/* =============================================================================
 *  AWS Lambda entry point — WebSocket gateway
 * =============================================================================
 *  Wire this as the integration for your WebSocket routes ($connect/$disconnect/
 *  $default). It does ONE thing: dispatch the API Gateway event through the
 *  GatewayBridge and return the response.
 *
 *  The bridge is instantiated OUTSIDE the handler (module scope, reused across
 *  warm invocations) — a plain object, NOT pulled from the Nest DI container. The
 *  Nest app is built/initialized exactly once (createNestApp binds the gateways,
 *  incl. the rxjs topics, to this same bridge); the handler body stays trivial.
 * ========================================================================== */

import { createNestApp, createGatewayBridge } from '../..';
import { AppModule } from './app.module';

// Built once per warm container, outside the DI container.
const bridge = createGatewayBridge();

// Build + init the Nest app exactly once (dedupes concurrent cold starts).
let ready: Promise<unknown> | undefined;
const ensureReady = () => (ready ??= createNestApp(AppModule, bridge).then((app) => app.init()));

export const handler = async (event: any) => {
  await ensureReady();
  return bridge.dispatch(event);
};
