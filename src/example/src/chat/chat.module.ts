import { Module } from "@nestjs/common";
import { ChatService } from "./chat.service";
import { ChatGateway } from "./chat.gateway";
import { ChatResolver } from "./chat.resolver";
import { ConversationRepositoryProvider } from "./conversation.repository";

@Module({
  imports: [],
  providers: [ChatService, ChatGateway, ChatResolver, ConversationRepositoryProvider],
  exports: [ChatService],
})
export class ChatModule {}
