/* =============================================================================
 *  Conversation repository — persistence port + two implementations.
 * =============================================================================
 *  A "multi-chat" demo: many conversations, each its own scoped topic. The
 *  realtime scoping (who receives a conversation's messages) is the gateway's job
 *  via rooms — this layer is plain CRUD over conversations + their messages.
 * ========================================================================== */

import { randomUUID } from "crypto";
import { Injectable } from "@nestjs/common";
import { PROVIDER } from "../../..";

/** DI token for the repository port (demo-specific, lives with the feature). */
export const CONVERSATION_REPOSITORY = Symbol("CONVERSATION_REPOSITORY");

export interface Conversation {
  id: string;
  title: string;
  createdAt: number;
}

export interface ChatMessage {
  id: string;
  conversationId: string;
  from: string;
  text: string;
  createdAt: number;
}

export interface ConversationRepository {
  createConversation(input: { title: string }): Promise<Conversation>;
  listConversations(): Promise<Conversation[]>;
  addMessage(input: {
    conversationId: string;
    from: string;
    text: string;
  }): Promise<ChatMessage>;
  listMessages(conversationId: string): Promise<ChatMessage[]>;
}

@Injectable()
export class InMemoryConversationRepository implements ConversationRepository {
  private readonly conversations: Conversation[] = [];
  private readonly messages = new Map<string, ChatMessage[]>();

  async createConversation(input: { title: string }): Promise<Conversation> {
    const convo: Conversation = {
      id: randomUUID(),
      title: input.title,
      createdAt: Date.now(),
    };
    this.conversations.push(convo);
    this.messages.set(convo.id, []);
    return convo;
  }
  async listConversations(): Promise<Conversation[]> {
    return [...this.conversations];
  }
  async addMessage(input: {
    conversationId: string;
    from: string;
    text: string;
  }): Promise<ChatMessage> {
    const msg: ChatMessage = {
      id: randomUUID(),
      conversationId: input.conversationId,
      from: input.from,
      text: input.text,
      createdAt: Date.now(),
    };
    const list = this.messages.get(input.conversationId) ?? [];
    list.push(msg);
    this.messages.set(input.conversationId, list);
    return msg;
  }
  async listMessages(conversationId: string): Promise<ChatMessage[]> {
    return [...(this.messages.get(conversationId) ?? [])];
  }
}

@Injectable()
export class DynamoConversationRepository implements ConversationRepository {
  private doc: any;
  private readonly table = process.env.CHAT_TABLE ?? "chat";

  private client() {
    if (!this.doc) {
      const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
      const { DynamoDBDocumentClient } = require("@aws-sdk/lib-dynamodb");
      this.doc = DynamoDBDocumentClient.from(new DynamoDBClient({}));
    }
    return this.doc;
  }

  async createConversation(input: { title: string }): Promise<Conversation> {
    const { PutCommand } = require("@aws-sdk/lib-dynamodb");
    const convo: Conversation = {
      id: randomUUID(),
      title: input.title,
      createdAt: Date.now(),
    };
    // All conversation rows share one partition so they can be listed by Query.
    await this.client().send(
      new PutCommand({
        TableName: this.table,
        Item: { pk: "CONVO", sk: convo.id, ...convo },
      }),
    );
    return convo;
  }
  async listConversations(): Promise<Conversation[]> {
    const { QueryCommand } = require("@aws-sdk/lib-dynamodb");
    const out = await this.client().send(
      new QueryCommand({
        TableName: this.table,
        KeyConditionExpression: "pk = :pk",
        ExpressionAttributeValues: { ":pk": "CONVO" },
      }),
    );
    return (out.Items ?? []).map(
      (i: any): Conversation => ({
        id: i.id,
        title: i.title,
        createdAt: i.createdAt,
      }),
    );
  }
  async addMessage(input: {
    conversationId: string;
    from: string;
    text: string;
  }): Promise<ChatMessage> {
    const { PutCommand } = require("@aws-sdk/lib-dynamodb");
    const msg: ChatMessage = {
      id: randomUUID(),
      conversationId: input.conversationId,
      from: input.from,
      text: input.text,
      createdAt: Date.now(),
    };
    // Messages live under the conversation partition, sorted by time, so a Query
    // returns one conversation's history in order.
    await this.client().send(
      new PutCommand({
        TableName: this.table,
        Item: {
          pk: `CONVO#${msg.conversationId}`,
          sk: `${msg.createdAt}#${msg.id}`,
          ...msg,
        },
      }),
    );
    return msg;
  }
  async listMessages(conversationId: string): Promise<ChatMessage[]> {
    const { QueryCommand } = require("@aws-sdk/lib-dynamodb");
    const out = await this.client().send(
      new QueryCommand({
        TableName: this.table,
        KeyConditionExpression: "pk = :pk",
        ExpressionAttributeValues: { ":pk": `CONVO#${conversationId}` },
      }),
    );
    return (out.Items ?? []).map(
      (i: any): ChatMessage => ({
        id: i.id,
        conversationId: i.conversationId,
        from: i.from,
        text: i.text,
        createdAt: i.createdAt,
      }),
    );
  }
}

/** Provider binding chosen by RT_PROVIDER. */
export const ConversationRepositoryProvider = {
  provide: CONVERSATION_REPOSITORY,
  useClass:
    PROVIDER === "aws"
      ? DynamoConversationRepository
      : InMemoryConversationRepository,
};
