/* =============================================================================
 *  AppModule — the demo's composition root.
 * =============================================================================
 *  Two protocols share one API Gateway socket:
 *    - @WebSocketGateway classes speak this library's {event,data} frames;
 *    - the GraphQL resolvers speak graphql-transport-ws.
 *  GatewayBridge.dispatch offers each frame to the graphql-ws handler first and
 *  falls back to @SubscribeMessage routing.
 *
 *  Note what is NOT here: no subscription-transport module, no adapter swap.
 *  GraphQLModule.forRoot is the stock Nest setup, and the only concession to
 *  running on Lambda is ApiGwPubSub in PostModule's providers.
 * ========================================================================== */

import { Module } from '@nestjs/common';
import { GraphQLModule } from '@nestjs/graphql';
import { ApolloDriver, ApolloDriverConfig } from '@nestjs/apollo';
import { apiGwPubSubContext } from '../../graphql';
import { pubsub } from './pubsub';
import { PostModule } from './posts/post.module';
import { ChatModule } from './chat/chat.module';

@Module({
  imports: [
    GraphQLModule.forRoot<ApolloDriverConfig>({
      driver: ApolloDriver,
      // In memory — Lambda's filesystem is read-only outside /tmp, so never
      // point this at a .gql file path.
      autoSchemaFile: true,
      sortSchema: true,
      // Deliberately no `subscriptions: { 'graphql-ws': true }`: that starts
      // graphql-ws's own server on a long-lived http.Server, which is the one
      // thing a Lambda behind API Gateway doesn't have. GraphQLWsHandler takes
      // over that role — see src/graphql/handler.ts.
      //
      // Publishes `pubsub` on the context so resolvers use @Context('pubsub')
      // and name no transport. Only needed for the HTTP endpoint: operations
      // arriving over the socket get it automatically, since the WebSocket path
      // bypasses Apollo's request pipeline (and therefore this factory).
      context: apiGwPubSubContext(pubsub),
    }),
    PostModule,
    ChatModule,
  ],
})
export class AppModule {}
