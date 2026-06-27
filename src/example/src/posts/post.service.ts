import { Injectable, Inject } from '@nestjs/common';
import { POST_REPOSITORY, PostRepository } from './post.repository';

@Injectable()
export class PostService {
  constructor(@Inject(POST_REPOSITORY) private readonly repo: PostRepository) {}
  create(input: { title: string; body: string }) {
    return this.repo.create(input);
  }
  list() {
    return this.repo.list();
  }
  remove(id: string) {
    return this.repo.remove(id);
  }
}
