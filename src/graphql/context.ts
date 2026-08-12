/* =============================================================================
 *  GraphQL context — hand resolvers the PubSub instead of injecting it.
 * =============================================================================
 *  A resolver that injects `ApiGwPubSub` is coupled to this transport. A resolver
 *  that reads it off the context is not:
 *
 *      import { PubSub } from 'graphql-subscriptions';
 *
 *      @Subscription(() => Comment, { filter: ... })
 *      commentAdded(@Args('title') title: string, @Context('pubsub') pubsub: PubSub) {
 *        return pubsub.asyncIterableIterator('commentAdded');
 *      }
 *
 *  Note the type: graphql-subscriptions' own PubSub, which ApiGwPubSub extends.
 *  That is the ordinary Apollo/Nest shape, so the SAME resolver runs on a
 *  long-lived server with the in-process PubSub (or a Redis one) and on Lambda
 *  with ApiGwPubSub — swapping providers means editing the context factory,
 *  nothing else. Which is this library's whole thesis, applied to GraphQL: one
 *  codebase, three runtimes.
 *
 *  ONE DIRECTION OF THAT SWAP IS A LIE, and it's worth being blunt about: going
 *  from ApiGwPubSub to an in-process PubSub only works on a runtime that HOLDS
 *  the socket. On Lambda the in-process iterator dies with the invocation and the
 *  subscription silently never fires. The portability is real; the constraint is
 *  the runtime, not the code.
 *
 *  YOU USUALLY DON'T NEED THIS FILE. Operations arriving over the API Gateway
 *  socket get `pubsub` in their context automatically (see handler.ts) —
 *  `@Context('pubsub')` just works. This helper is for the OTHER path: the HTTP
 *  GraphQL endpoint in ECS/HTTP mode, whose context is built by Apollo's own
 *  factory, which the WebSocket path deliberately bypasses.
 *
 *  THE ORDERING PROBLEM, AND WHY YOU PASS THE INSTANCE IN. GraphQLModule.forRoot
 *  is evaluated when your AppModule is defined — before the app exists, and long
 *  before enableGraphQLSubscriptions() could have created anything. This used to
 *  be solved with a module-level singleton resolved lazily at request time, which
 *  is precisely the hidden global that made "which PubSub is this?" unanswerable.
 *
 *  So: create the PubSub yourself, next to the bridge, and hand it to both.
 *
 *      // pubsub.ts
 *      export const pubsub = new ApiGwPubSub();
 *
 *      // app.module.ts
 *      GraphQLModule.forRoot({ ..., context: apiGwPubSubContext(pubsub) })
 *
 *      // handler.ts
 *      enableGraphQLSubscriptions(app, bridge, { pubsub });
 *
 *  Two references to one object, both visible. Nothing is resolved behind your
 *  back, and a second bridge in the same process cannot quietly steal it.
 * ========================================================================== */

import { ApiGwPubSub } from './pubsub';
import { PUBSUB_CONTEXT_KEY } from './tokens';

/**
 * Wrap (or create) the `context` factory passed to `GraphQLModule.forRoot`, so
 * the HTTP path publishes the same PubSub the WebSocket path does:
 *
 *     GraphQLModule.forRoot<ApolloDriverConfig>({
 *       driver: ApolloDriver,
 *       autoSchemaFile: true,
 *       context: apiGwPubSubContext(pubsub),   // or: apiGwPubSubContext(pubsub, myContextFn)
 *     })
 *
 * `pubsub` is applied last, so a stray key of the same name in your own factory
 * can't silently replace it with something that doesn't work here.
 */
export function apiGwPubSubContext<T extends object>(
  pubsub: ApiGwPubSub,
  context?: (...args: any[]) => T | Promise<T>,
) {
  return async (...args: any[]): Promise<T & Record<string, unknown>> => ({
    ...(context ? await context(...args) : ({} as T)),
    [PUBSUB_CONTEXT_KEY]: pubsub,
  });
}
