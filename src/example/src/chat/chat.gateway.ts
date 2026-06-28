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
import { ChatService } from "./chat.service";

/** The room name for a conversation's scoped topic. Sentinel-prefixed so it
 *  can't collide with other rooms (e.g. the posts feed or the global channel). */
const roomOf = (conversationId: string) => `conv#${conversationId}`;

@WebSocketGateway()
export class ChatGateway {
  @WebSocketServer() server: GatewayServer;

  constructor(@Inject(ChatService) private readonly chat: ChatService) {}

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
  // A client in a different conversation (or none) never receives it.
  @SubscribeMessage("chat.send")
  async send(
    @MessageBody() data: { conversationId: string; from: string; text: string },
    @Ack() ack: (response: WsResponse) => void,
  ) {
    const message = await this.chat.sendMessage(data);
    await this.server.to(roomOf(message.conversationId)).emit("chat.message", message);
    ack({ event: "chat.send.ack", data: { id: message.id } });
  }
}
