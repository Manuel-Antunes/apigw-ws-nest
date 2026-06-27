/* =============================================================================
 *  PostGateway — two coexisting NestJS WebSocket patterns over API Gateway.
 * =============================================================================
 *  1) rxjs streams + @Ack (NestJS 11):
 *       - a plain BehaviorSubject of posts, mapped to Observable<WsResponse> by a
 *         private function; returning it from a handler streams to the caller.
 *       - @Ack() is the immediate acknowledgement, separate from the stream.
 *
 *  2) the classic server/client API (Socket.IO-shaped):
 *       - @WebSocketServer() server.to(room).emit(...) to broadcast.
 *       - @ConnectedSocket() client.join(room) to subscribe.
 *
 *  Both run side by side without interfering. Nothing here knows about API
 *  Gateway, DynamoDB, or the @connections API — only rxjs + NestJS.
 * ========================================================================== */

import { Inject } from "@nestjs/common";
import {
  WebSocketGateway,
  WebSocketServer,
  SubscribeMessage,
  MessageBody,
  ConnectedSocket,
  Ack,
  WsResponse,
} from "@nestjs/websockets";
import { BehaviorSubject, Observable } from "rxjs";
import { filter, map } from "rxjs/operators";
import { GatewayClient, GatewayServer } from "../../..";
import { PostService } from "./post.service";
import { Post } from "./post.repository";

@WebSocketGateway()
export class PostGateway {
  /** Classic broadcast surface (used by post.delete). */
  @WebSocketServer() server: GatewayServer;

  /** Plain rxjs BehaviorSubject: the latest created post (null until the first). */
  private readonly posts$ = new BehaviorSubject<Post | null>(null);

  constructor(@Inject(PostService) private readonly posts: PostService) {}

  /** Map the posts BehaviorSubject to an Observable<WsResponse>. */
  private feed(): Observable<WsResponse> {
    return this.posts$.pipe(
      filter((post): post is Post => post !== null),
      map(post => ({ event: "post.created", data: post })),
    );
  }

  // ---- rxjs pattern -------------------------------------------------------

  // Subscribe: join the 'posts' room (so classic emits like post.deleted reach
  // this client too) AND return the rxjs feed (Observable<WsResponse>).
  @SubscribeMessage("post.subscribe")
  subscribe(
    @ConnectedSocket() client: GatewayClient,
    @Ack() ack: (response: WsResponse) => void,
  ) {
    client.join("posts");
    ack({ event: "post.subscribed", data: { room: "posts" } });
    return this.feed();
  }

  // Create: push the new post onto the subject; the feed streams 'post.created'.
  @SubscribeMessage("post.create")
  async create(
    @MessageBody() data: { title: string; body: string },
    @Ack() ack: (response: WsResponse) => void,
  ) {
    const post = await this.posts.create(data);
    this.posts$.next(post);
    ack({ event: "post.create.ack", data: { id: post.id } });
  }

  // ---- classic server/client pattern (coexists with the rxjs one) ---------

  // Delete: broadcast 'post.deleted' to the room via the @WebSocketServer().
  @SubscribeMessage("post.delete")
  async deletePost(@MessageBody() data: { id: string }) {
    await this.posts.remove(data.id);
    await this.server.to("posts").emit("post.deleted", { id: data.id });
    return { event: "post.delete.ack", data: { id: data.id } }; // plain return = ack to caller
  }

  // No @Ack: the returned value IS the response (classic request/response).
  @SubscribeMessage("post.list")
  async listPosts(): Promise<WsResponse<Post[]>> {
    return { event: "post.list", data: await this.posts.list() };
  }
}
