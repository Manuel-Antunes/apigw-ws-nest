/* =============================================================================
 *  apigw-ws-nest/graphql — GraphQL subscriptions over API Gateway WebSocket.
 * =============================================================================
 *  A separate entry point on purpose: it pulls in `graphql`, `graphql-ws` and
 *  `@nestjs/graphql`, which the core transport neither needs nor should force on
 *  consumers.
 *
 *  One call, where you already build the bridge:
 *
 *      import { enableGraphQLSubscriptions } from 'apigw-ws-nest/graphql';
 *
 *      const bridge = createGatewayBridge();
 *      const app = await createNestApp(AppModule, bridge);
 *      await app.init();
 *      enableGraphQLSubscriptions(app, bridge);
 *
 *  Resolvers then read the PubSub off the context — `@Context('pubsub')` — and
 *  name no transport at all. See pubsub.ts for why the PubSub must be
 *  substituted, and transport.ts for what happens on the wire.
 * ========================================================================== */

export { enableGraphQLSubscriptions } from './enable';
export type { EnableGraphQLSubscriptionsOptions } from './enable';
export { ApiGwPubSub, apiGwPubSub, captureTopics, replayPayload } from './pubsub';
export { apiGwPubSubContext } from './context';
export { GraphQLWsTransport } from './transport';
export type { ApiGwGraphQLOptions, TransportDeps } from './transport';
export {
  InMemorySubscriptionRegistry,
  DynamoSubscriptionRegistry,
  subscriptionRegistry,
} from './subscription-registry';
export type {
  GqlSubscriptionRegistry,
  GqlSubscriptionRecord,
} from './subscription-registry';
export { PUBSUB_CONTEXT_KEY } from './tokens';
export {
  GRAPHQL_TRANSPORT_WS_PROTOCOL,
  MessageType,
  isClientMessage,
} from './protocol';
export type { Message, SubscribePayload } from './protocol';
