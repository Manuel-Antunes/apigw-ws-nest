/* =============================================================================
 *  AWS Lambda entry point — ONE export, every trigger.
 * =============================================================================
 *  $connect, $disconnect, $default AND the Messages stream all point at this same
 *  function. bridge.serve() routes by event shape.
 *
 *  That is deliberate, and it is the recommended layout. Both halves need the
 *  same booted Nest app — fan-out re-executes a subscription per subscriber, so
 *  it needs the schema and the DI container — and a Function per trigger means
 *  each one keeps its own, mostly-cold pool. Sharing one Function shares one warm
 *  pool: the container that just served the mutation is a candidate to serve the
 *  stream record it produced, with the app already up.
 *
 *  The bridge is assembled OUTSIDE the handler (module scope, reused across warm
 *  invocations) and owns everything: store, publisher, bus, ledger, flusher and
 *  the protocols registered on it. Nothing is a global.
 *
 *  Below is an ordinary NestJS bootstrap; the only unusual line is
 *  useWebSocketAdapter, exactly as you'd swap in Socket.IO's IoAdapter.
 * ========================================================================== */

import { NestFactory } from '@nestjs/core';
import { ApiGatewayWsAdapter, GatewayBridge } from '../..';
import { enableGraphQLSubscriptions } from '../../graphql';
import { AppModule } from './app.module';
import { pubsub } from './pubsub';

// Built once per warm container. `.provider('aws')` picks the matched set of
// backends (DynamoDB store, @connections publisher, outbox bus, Dynamo ledger);
// any of them can be replaced individually with .store()/.publisher()/etc.
const bridge = GatewayBridge.builder().provider('aws').build();

async function bootstrap() {
  const app = await NestFactory.create(AppModule, { logger: ['error', 'warn'] });
  // BEFORE init(): registers the {event,data} protocol on the bridge and binds
  // the @SubscribeMessage handlers to THIS bridge. The defaults are the Lambda
  // ones: $connect runs each gateway's handleConnection, awaited (throwing
  // refuses the socket), and there is no HTTP dispatch route to forge into.
  app.useWebSocketAdapter(new ApiGatewayWsAdapter(app, bridge));
  await app.init();
  // AFTER init(): GraphQLModule has built the schema by now. Same `pubsub` the
  // AppModule put on the HTTP context, so both paths publish through one object.
  enableGraphQLSubscriptions(app, bridge, { pubsub });
  return app;
}

// Built exactly once per warm container (dedupes concurrent cold starts).
let ready: Promise<unknown> | undefined;

/**
 * Every trigger.
 *
 *   an API Gateway event (requestContext)  -> bridge.dispatch -> a response
 *   a DynamoDB Stream event (Records)      -> bridge.flush    -> { batchItemFailures }
 *
 * A publish (server.to(room).emit / pubsub.publish) does not deliver anything
 * itself — it writes one outbox row and returns. The stream branch is what
 * delivers, batched per topic and retried by Lambda until it succeeds.
 */
export const handler = async (event: any, context?: any) => {
  await (ready ??= bootstrap());
  return bridge.serve(event, context);
};
