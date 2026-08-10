/* =============================================================================
 *  PostModule — one feature, two protocols on the same API Gateway socket.
 * =============================================================================
 *    PostGateway  — @WebSocketGateway, this library's {event,data} frames
 *    PostResolver — @Resolver, GraphQL queries/mutations/subscriptions
 *
 *  Both share PostService, so a post created over GraphQL shows up in the plain
 *  WebSocket client's post.list and vice versa.
 *
 *  Note there is NOTHING here about GraphQL subscriptions — no PubSub provider,
 *  no module. That wiring is one call at the composition root, next to
 *  createGatewayBridge(): enableGraphQLSubscriptions(app, bridge). The resolver
 *  receives the PubSub on the GraphQL context.
 * ========================================================================== */

import { Module } from "@nestjs/common";
import { PostService } from "./post.service";
import { PostGateway } from "./post.gateway";
import { PostResolver } from "./post.resolver";
import { PostRepositoryProvider } from "./post.repository";

@Module({
  imports: [],
  providers: [PostService, PostGateway, PostResolver, PostRepositoryProvider],
  exports: [PostService],
})
export class PostModule {}
