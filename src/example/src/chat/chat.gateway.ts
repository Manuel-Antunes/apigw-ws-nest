/* =============================================================================
 *  ChatGateway — multi-conversation chat, each conversation a SCOPED topic.
 * =============================================================================
 *  Demonstrates per-topic scoping on top of the same bridge as PostGateway
 *  (two gateways, one connection — the adapter merges their routes):
 *
 *    - The conversation DIRECTORY is GLOBAL: chat.create broadcasts
 *      'chat.conversation.created' via server.emit(...) so every client's list
 *      updates. Knowing a room exists is public.
 *    - The conversation MESSAGES are SCOPED: a client must chat.join a specific
 *      conversation (client.join its room) to receive its 'chat.message' frames.
 *      chat.send broadcasts only to that room — someone in another conversation
 *      (or who never joined) never sees it.
 *
 *  Scoping is BACKEND state (room membership in the ConnectionStore), not a
 *  client-side `on(...)`: the server decides who receives each message.
 *
 *  IDENTITY is established once, at $connect (handleConnection, run by the
 *  adapter's lifecycle: 'connect'), and read by a guard on every later frame —
 *  so chat.send's `from` is who the server says you are, not what you claim.
 * ========================================================================== */

import { Inject, UnauthorizedException, UseGuards } from "@nestjs/common";
import {
  WebSocketGateway,
  WebSocketServer,
  SubscribeMessage,
  MessageBody,
  ConnectedSocket,
  Ack,
  OnGatewayConnection,
  WsResponse,
} from "@nestjs/websockets";
import { GatewayClient, GatewayServer } from "../../..";
import { ChatService } from "./chat.service";
import { WsUserGuard } from "./ws-user.guard";

/** What handleConnection puts in client.data. Ids and claims — never tokens. */
export interface ChatIdentity {
  user: { name: string };
}

/** The demo's "identity provider": `?token=demo:<name>`. A real app verifies a
 *  signed, short-lived token here. */
const DEMO_TOKEN = /^demo:(.{1,40})$/;

/** The room name for a conversation's scoped topic. Sentinel-prefixed so it
 *  can't collide with other rooms (e.g. the posts feed or the global channel). */
const roomOf = (conversationId: string) => `conv#${conversationId}`;

@WebSocketGateway()
export class ChatGateway implements OnGatewayConnection {
  @WebSocketServer() server: GatewayServer;

  constructor(@Inject(ChatService) private readonly chat: ChatService) {}

  // Runs ONCE per connection, at $connect, awaited. Throwing refuses the socket
  // (401 here) and nothing is stored. No token = an anonymous connection, which
  // may read but not send (see WsUserGuard).
  async handleConnection(client: GatewayClient<ChatIdentity>) {
    const token = client.handshake.query.token;
    if (!token) return;
    const match = DEMO_TOKEN.exec(token);
    if (!match) throw new UnauthorizedException("invalid token");
    client.data.user = { name: match[1] }; // persisted with the connection
    // Recorded now, applied once accepted: server.to('user:<name>') then
    // reaches every socket this user has open.
    await client.join(`user:${match[1]}`);
  }

  // Who the server thinks you are — on whichever instance serves the frame.
  @SubscribeMessage("chat.whoami")
  whoami(@ConnectedSocket() client: GatewayClient<Partial<ChatIdentity>>): WsResponse {
    return { event: "chat.whoami", data: client.data.user ?? null };
  }

  // List existing conversations (used to populate a freshly-connected client).
  @SubscribeMessage("chat.conversations")
  async conversations(): Promise<WsResponse> {
    return { event: "chat.conversations", data: await this.chat.listConversations() };
  }

  // Create a conversation, then announce it GLOBALLY so every connected client
  // can show it in their directory. @Ack returns the new conversation to the
  // caller (so it can immediately join).
  @SubscribeMessage("chat.create")
  async create(
    @MessageBody() data: { title: string },
    @Ack() ack: (response: WsResponse) => void,
  ) {
    const conversation = await this.chat.createConversation(data);
    await this.server.emit("chat.conversation.created", conversation);
    ack({ event: "chat.created", data: conversation });
  }

  // SCOPED subscribe: join this conversation's room and return its history. From
  // now on this connection receives the conversation's chat.message broadcasts,
  // from any instance, because the membership is durable in the store.
  @SubscribeMessage("chat.join")
  async join(
    @ConnectedSocket() client: GatewayClient,
    @MessageBody() data: { conversationId: string },
  ): Promise<WsResponse> {
    await client.join(roomOf(data.conversationId));
    const messages = await this.chat.history(data.conversationId);
    return {
      event: "chat.joined",
      data: { conversationId: data.conversationId, messages },
    };
  }

  // Leave a conversation's room — stop receiving its messages.
  @SubscribeMessage("chat.leave")
  async leave(
    @ConnectedSocket() client: GatewayClient,
    @MessageBody() data: { conversationId: string },
  ): Promise<WsResponse> {
    await client.leave(roomOf(data.conversationId));
    return { event: "chat.left", data: { conversationId: data.conversationId } };
  }

  // Send a message — persisted, then broadcast ONLY to that conversation's room.
  // A client in a different conversation (or none) never receives it. Only for
  // identified connections; `from` is the identity, whatever the body claims.
  @UseGuards(WsUserGuard)
  @SubscribeMessage("chat.send")
  async send(
    @ConnectedSocket() client: GatewayClient<ChatIdentity>,
    @MessageBody() data: { conversationId: string; text: string },
    @Ack() ack: (response: WsResponse) => void,
  ) {
    const message = await this.chat.sendMessage({
      conversationId: data.conversationId,
      from: client.data.user.name,
      text: data.text,
    });
    await this.server.to(roomOf(message.conversationId)).emit("chat.message", message);
    ack({ event: "chat.send.ack", data: { id: message.id } });
  }
}
