/* =============================================================================
 *  AWS Lambda entry point — WebSocket gateway
 * =============================================================================
 *  Wire this as the integration for your WebSocket routes ($connect/$disconnect/
 *  $default). It does ONE thing: dispatch the API Gateway event through the
 *  GatewayBridge and return the response.
 *
 *  The bridge is instantiated OUTSIDE the handler (module scope, reused across
 *  warm invocations) — a plain object, NOT pulled from the Nest DI container.
 *
 *  Everything below is an ordinary NestJS bootstrap; the only unusual line is
 *  useWebSocketAdapter, exactly as you'd swap in Socket.IO's IoAdapter. (The
 *  library also ships createNestApp(AppModule, bridge) as a shorthand for those
 *  two lines — this spells it out so nothing is hidden.)
 * ========================================================================== */

import { NestFactory } from '@nestjs/core';
import { ApiGatewayWsAdapter, createGatewayBridge } from '../..';
import { enableGraphQLSubscriptions } from '../../graphql';
import { AppModule } from './app.module';

// Built once per warm container, outside the DI container.
const bridge = createGatewayBridge();

async function bootstrap() {
  const app = await NestFactory.create(AppModule, { logger: ['error', 'warn'] });
  // BEFORE init(): keeps the adapter's raw dispatch route ahead of Nest's 404
  // catch-all, and binds the @SubscribeMessage handlers to THIS bridge.
  app.useWebSocketAdapter(new ApiGatewayWsAdapter(app, bridge));
  await app.init();
  // AFTER init(): GraphQLModule has built the schema by now — which is why this
  // is a plain call here rather than an onModuleInit somewhere.
  enableGraphQLSubscriptions(app, bridge);
  return app;
}

// Built exactly once per warm container (dedupes concurrent cold starts).
let ready: Promise<unknown> | undefined;

export const handler = async (event: any) => {
  await (ready ??= bootstrap());
  return bridge.dispatch(event);
};
