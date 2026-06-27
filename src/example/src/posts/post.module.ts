import { Module } from "@nestjs/common";
import { PostService } from "./post.service";
import { PostGateway } from "./post.gateway";
import { PostRepositoryProvider } from "./post.repository";

@Module({
  imports: [],
  providers: [PostService, PostGateway, PostRepositoryProvider],
  exports: [PostService],
})
export class PostModule {}
