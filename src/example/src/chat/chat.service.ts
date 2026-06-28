import { Injectable, Inject } from "@nestjs/common";
import {
  CONVERSATION_REPOSITORY,
  ConversationRepository,
} from "./conversation.repository";

@Injectable()
export class ChatService {
  constructor(
    @Inject(CONVERSATION_REPOSITORY)
    private readonly repo: ConversationRepository,
  ) {}

  createConversation(input: { title: string }) {
    return this.repo.createConversation(input);
  }
  listConversations() {
    return this.repo.listConversations();
  }
  sendMessage(input: { conversationId: string; from: string; text: string }) {
    return this.repo.addMessage(input);
  }
  history(conversationId: string) {
    return this.repo.listMessages(conversationId);
  }
}
