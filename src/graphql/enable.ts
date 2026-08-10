/* =============================================================================
 *  enableGraphQLSubscriptions — one call, at the composition root.
 * =============================================================================
 *      const bridge = createGatewayBridge();
 *      const app = await createNestApp(AppModule, bridge);
 *      await app.init();
 *      enableGraphQLSubscriptions(app, bridge);   // <- the entire wiring
 *
 *  No module, no provider, no onModuleInit. It sits exactly where
 *  createGatewayBridge() already sits — the transport is built outside DI,
 *  because the Lambda handler needs it before a Nest app exists.
 *
 *  WHY IT CAN'T COME FROM THE GRAPHQL CONTEXT. The obvious wish is to let
 *  GraphQL hand us everything GraphQL needs — schema included — via the context
 *  factory. But the context is only built once we are ALREADY executing an
 *  operation, and to know that an inbound API Gateway frame *is* an operation,
 *  something must have inspected the frame first. That inspector is the
 *  GatewayBridge frame handler, which is strictly upstream of the context:
 *
 *      frame handler registered  ->  frame recognised as graphql-transport-ws
 *                                ->  operation executed  ->  context built
 *
 *  So the registration can't be bootstrapped from the thing it produces. What it
 *  CAN be is explicit and late: called after app.init(), the schema is already
 *  built, so there is no lazy getter and no lifecycle-ordering guesswork — the
 *  reason this reads better than the onModuleInit it replaces.
 *
 *  (And for the record: routing this through Apollo instead is not an option
 *  even if you wanted to. ApolloServer#executeOperation does not run a
 *  subscription's `subscribe` at all — it executes the operation like a query,
 *  so `resolve` receives undefined. Apollo's own recommended subscription setup,
 *  graphql-ws + useServer, bypasses Apollo Server and calls graphql-js directly.
 *  That is exactly what transport.ts does.)
 * ========================================================================== */

import type { INestApplication } from '@nestjs/common';
import { GraphQLSchemaHost } from '@nestjs/graphql';

import { GatewayBridge } from '../index';
import { ApiGwPubSub, ensureApiGwPubSub } from './pubsub';
import { ApiGwGraphQLOptions, GraphQLWsTransport } from './transport';
import { GqlSubscriptionRegistry, subscriptionRegistry } from './subscription-registry';

export interface EnableGraphQLSubscriptionsOptions extends ApiGwGraphQLOptions {
  /** Swap the durable subscription registry (defaults to DynamoDB in aws mode,
   *  in-memory in local mode — mirroring RT_PROVIDER). */
  registry?: GqlSubscriptionRegistry;
}

/**
 * Teach an initialized Nest app to serve GraphQL subscriptions over the API
 * Gateway socket. Returns the PubSub, which is also published on every GraphQL
 * context under `pubsub`.
 *
 * MUST be called after `app.init()` (or `app.listen()`): the schema is built by
 * GraphQLModule during its own init, and this reads it eagerly so a misordered
 * call fails at boot rather than on the first client frame.
 */
export function enableGraphQLSubscriptions(
  app: INestApplication,
  bridge: GatewayBridge,
  options: EnableGraphQLSubscriptionsOptions = {},
): ApiGwPubSub {
  const { registry, ...transportOptions } = options;

  let host: GraphQLSchemaHost;
  try {
    // strict:false — we needn't sit in GraphQLModule's module graph.
    host = app.get(GraphQLSchemaHost, { strict: false });
  } catch {
    throw new Error(
      'enableGraphQLSubscriptions(): GraphQLSchemaHost is not available. Import GraphQLModule.forRoot(...) ' +
        'in your application module.',
    );
  }
  // Touch it now: the getter throws while the schema is still unbuilt, and a
  // clear message here beats an obscure failure on the first subscribe frame.
  try {
    void host.schema;
  } catch {
    throw new Error(
      'enableGraphQLSubscriptions(): the GraphQL schema has not been built yet. Call this AFTER ' +
        'app.init() (Lambda) or app.listen() (HTTP).',
    );
  }

  const transport = GraphQLWsTransport.attach(bridge, {
    schema: () => host.schema,
    registry: registry ?? subscriptionRegistry(),
    options: transportOptions,
  });

  // Re-bound on every call, so the local emulator's /__reload — which builds a
  // brand-new bridge and app — keeps publishing through the live transport.
  const pubsub = ensureApiGwPubSub();
  pubsub.bindTransport(transport);
  return pubsub;
}
