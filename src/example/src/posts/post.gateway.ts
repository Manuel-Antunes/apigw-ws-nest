/* =============================================================================
 *  PostGateway — NestJS WebSocket gateway over API Gateway.
 * =============================================================================
 *  Written against the STANDARD Socket.IO-shaped surface — @WebSocketServer()
 *  server.to(room).emit(...) to broadcast, @ConnectedSocket() client.join(room)
 *  to subscribe. Nothing here knows about API Gateway, DynamoDB, or the
 *  @connections API; the adapter + bridge make it work over Lambda.
 *
 *  TWO durable, cross-instance channels — both backed by the ConnectionStore:
 *    - GLOBAL:   server.emit('post.created', post) reaches EVERY connection.
 *                No subscribe needed — every socket is auto-joined to the global
 *                room on $connect (see GatewayBridge). New posts are a public feed.
 *    - SPECIFIC: client.join('posts') opts a connection in; server.to('posts')
 *                .emit('post.deleted', ...) reaches ONLY those who joined.
 *
 *  WHY NOT AN IN-PROCESS EventEmitter / rxjs Subject:
 *  Every Lambda invocation may run on a DIFFERENT (or freshly redeployed)
 *  container, and a container FREEZES the moment its handler returns. In-process
 *  pub/sub — an rxjs Subject, `fromEvent(emitter, ...)`, a Map of callbacks —
 *  lives and dies with ONE container, so it only ever reaches listeners in that
 *  SAME warm container; the instant the subscriber's container is replaced the
 *  broadcast is silently lost. The store-backed channels above don't have that
 *  problem: membership and delivery both go through state that outlives any one
 *  container (DynamoDB + the @connections API).
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
import { GatewayClient, GatewayServer } from "../../..";
import { PostService } from "./post.service";
import { Post } from "./post.repository";

/** The SPECIFIC room a client opts into via post.subscribe; only its members get
 *  post.deleted. (post.created is GLOBAL — every connection, no join needed.) */
const FEED_ROOM = "posts";

@WebSocketGateway()
export class PostGateway {
  /** Broadcast surface — durable, cross-instance fan-out via the ConnectionStore. */
  @WebSocketServer() server: GatewayServer;

  constructor(@Inject(PostService) private readonly posts: PostService) {}

  // Subscribe: opt INTO the specific 'posts' room. Used here for targeted
  // post.deleted notices — global post.created already reaches everyone.
  @SubscribeMessage("post.subscribe")
  subscribe(
    @ConnectedSocket() client: GatewayClient,
    @Ack() ack: (response: WsResponse) => void,
  ) {
    client.join(FEED_ROOM);
    ack({ event: "post.subscribed", data: { room: FEED_ROOM } });
  }

  // Create: persist the post, then broadcast 'post.created' GLOBALLY via
  // server.emit(...). Every connected client receives it — across instances and
  // without having subscribed — because all sockets are auto-joined to the
  // global room on $connect. @Ack is the immediate acknowledgement to the caller.
  @SubscribeMessage("post.create")
  async create(
    @MessageBody() data: { title: string; body: string },
    @Ack() ack: (response: WsResponse) => void,
  ) {
    const post = await this.posts.create(data);
    await this.server.emit("post.created", post);
    ack({ event: "post.create.ack", data: { id: post.id } });
  }

  // Delete: broadcast 'post.deleted' to the SPECIFIC room — only connections that
  // ran post.subscribe receive it.
  @SubscribeMessage("post.delete")
  async deletePost(@MessageBody() data: { id: string }) {
    await this.posts.remove(data.id);
    await this.server.to(FEED_ROOM).emit("post.deleted", { id: data.id });
    return { event: "post.delete.ack", data: { id: data.id } }; // plain return = ack to caller
  }

  // No @Ack: the returned value IS the response (classic request/response).
  @SubscribeMessage("post.list")
  async listPosts(): Promise<WsResponse<Post[]>> {
    return { event: "post.list", data: await this.posts.list() };
  }
}
