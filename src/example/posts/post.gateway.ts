/* =============================================================================
 *  PostGateway — a STANDARD NestJS WebSocket gateway.
 * =============================================================================
 *  Nothing here knows about API Gateway, DynamoDB, connections, or the @connections
 *  Management API. It uses only the portable NestJS/Socket.IO surface:
 *    - @WebSocketServer() server  -> server.to(room).emit(event, data)
 *    - @ConnectedSocket() client  -> client.join(room)
 *  The transport (ApiGatewayWsAdapter) persists rooms/connections and delivers
 *  messages on its behalf. Point Nest at IoAdapter (Socket.IO) instead and this
 *  file is byte-for-byte the same.
 * ========================================================================== */

import { Inject } from '@nestjs/common';
import {
  WebSocketGateway,
  WebSocketServer,
  SubscribeMessage,
  MessageBody,
  ConnectedSocket,
} from '@nestjs/websockets';
import { GatewayClient, GatewayServer } from '../../apigw-ws';
import { PostService } from './post.service';

@WebSocketGateway()
export class PostGateway {
  @WebSocketServer() server: GatewayServer;

  constructor(@Inject(PostService) private readonly posts: PostService) {}

  // Subscribe to the feed: { "event": "post.subscribe", "data": {} }
  @SubscribeMessage('post.subscribe')
  async subscribe(@ConnectedSocket() client: GatewayClient) {
    await client.join('posts');
    return { event: 'post.subscribed', data: { room: 'posts' } }; // ack to caller
  }

  // Create a post: { "event": "post.create", "data": { title, body } }
  // The handler writes, then broadcasts to the room — exactly like a Socket.IO
  // gateway. No DynamoDB Streams, no separate consumer.
  @SubscribeMessage('post.create')
  async create(@MessageBody() data: { title: string; body: string }) {
    const post = await this.posts.create(data);
    await this.server.to('posts').emit('post.created', post); // fan-out to subscribers
    return { event: 'post.create.ack', data: { id: post.id } }; // ack to caller
  }

  @SubscribeMessage('post.list')
  async listPosts() {
    return { event: 'post.list', data: await this.posts.list() };
  }
}
