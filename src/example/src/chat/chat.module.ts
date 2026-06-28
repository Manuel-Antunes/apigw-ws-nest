import { Module } from "@nestjs/common";
import { ChatService } from "./chat.service";
import { ChatGateway } from "./chat.gateway";
import { ConversationRepositoryProvider } from "./conversation.repository";

@Module({
  imports: [],
  providers: [ChatService, ChatGateway, ConversationRepositoryProvider],
  exports: [ChatService],
})
export class ChatModule {}
