/* =============================================================================
 *  AWS Lambda entry point — WebSocket gateway only
 * =============================================================================
 *  Wire this as the integration for your WebSocket routes ($connect/$disconnect/
 *  $default). It does ONE thing: dispatch the API Gateway event through the
 *  GatewayBridge and return the response.
 *
 *  The bridge is instantiated OUTSIDE the handler (module scope, reused across
 *  warm invocations) — a plain object, NOT pulled from the Nest DI container.
 *  The Nest app is built once and binds the @SubscribeMessage handlers to this
 *  same bridge's hub; the handler body stays trivial.
 * ========================================================================== */

import { createNestApp, createGatewayBridge } from './apigw-ws';
import { AppModule } from './example/app.module';

// Built once per warm container, outside the DI container.
const bridge = createGatewayBridge();

// Bind the gateway handlers to this bridge exactly once (dedupes cold starts).
let ready: Promise<unknown> | undefined;
const ensureReady = () =>
  (ready ??= createNestApp(AppModule, bridge).then((app) => app.init()));

export const handler = async (event: any) => {
  await ensureReady();
  return bridge.dispatch(event);
};
