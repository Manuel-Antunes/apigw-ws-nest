/* =============================================================================
 *  apigw-ws-nest/graphql — GraphQL subscriptions over API Gateway WebSocket.
 * =============================================================================
 *  A separate entry point on purpose: it pulls in `graphql`, `graphql-ws` and
 *  `@nestjs/graphql`, which the core transport neither needs nor should force on
 *  consumers.
 *
 *  Same shape as graphql-sse: hand a handler the schema, register it, done.
 *
 *      import { createGraphQLWsHandler } from 'apigw-ws-nest/graphql';
 *
 *      const { schema } = app.get(GraphQLSchemaHost);
 *      bridge.use(createGraphQLWsHandler({ schema }));
 *
 *  ...or let enableGraphQLSubscriptions() read the schema off the app for you:
 *
 *      const pubsub = enableGraphQLSubscriptions(app, bridge);
 *
 *  Resolvers then read the PubSub off the context — `@Context('pubsub')` — and
 *  name no transport at all. See pubsub.ts for why the PubSub must be
 *  substituted, and handler.ts for what happens on the wire.
 * ========================================================================== */

export {
  GraphQLWsHandler,
  createGraphQLWsHandler,
  enableGraphQLSubscriptions,
} from './handler';
export type {
  GraphQLWsOptions,
  EnableGraphQLSubscriptionsOptions,
} from './handler';

export { ApiGwPubSub } from './pubsub';
export { apiGwPubSubContext } from './context';

export {
  InMemorySubscriptionRegistry,
  DynamoSubscriptionRegistry,
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
} from './messages';
export type { Message, SubscribePayload } from './messages';
