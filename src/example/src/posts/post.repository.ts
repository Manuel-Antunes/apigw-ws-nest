/* =============================================================================
 *  Post repository — the demo's persistence port + two implementations.
 * =============================================================================
 *  Plain CRUD: create + list. The realtime broadcast of 'post.created' is done by
 *  the gateway (server.to('posts').emit) right after create — no stream involved.
 * ========================================================================== */

import { randomUUID } from 'crypto';
import { Injectable } from '@nestjs/common';
import { PROVIDER } from '../../..';

/** DI token for the repository port (demo-specific, lives with the feature). */
export const POST_REPOSITORY = Symbol('POST_REPOSITORY');

export interface Post {
  id: string;
  title: string;
  body: string;
  createdAt: number;
}

export interface PostRepository {
  create(input: { title: string; body: string }): Promise<Post>;
  list(): Promise<Post[]>;
  remove(id: string): Promise<void>;
}

@Injectable()
export class InMemoryPostRepository implements PostRepository {
  private readonly posts: Post[] = [];

  async create(input: { title: string; body: string }): Promise<Post> {
    const post: Post = {
      id: randomUUID(),
      title: input.title,
      body: input.body,
      createdAt: Date.now(),
    };
    this.posts.push(post);
    return post;
  }
  async list() {
    return [...this.posts];
  }
  async remove(id: string) {
    const i = this.posts.findIndex((p) => p.id === id);
    if (i >= 0) this.posts.splice(i, 1);
  }
}

@Injectable()
export class DynamoPostRepository implements PostRepository {
  private doc: any;
  private readonly table = process.env.POSTS_TABLE ?? 'posts';
  private client() {
    if (!this.doc) {
      const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
      const { DynamoDBDocumentClient } = require('@aws-sdk/lib-dynamodb');
      this.doc = DynamoDBDocumentClient.from(new DynamoDBClient({}));
    }
    return this.doc;
  }
  async create(input: { title: string; body: string }): Promise<Post> {
    const { PutCommand } = require('@aws-sdk/lib-dynamodb');
    const post: Post = { id: randomUUID(), createdAt: Date.now(), ...input };
    // ONLY write. The real DynamoDB Stream emits the change; the stream Lambda consumes it.
    await this.client().send(new PutCommand({ TableName: this.table, Item: post }));
    return post;
  }
  async list(): Promise<Post[]> {
    const { ScanCommand } = require('@aws-sdk/lib-dynamodb');
    const out = await this.client().send(new ScanCommand({ TableName: this.table }));
    return (out.Items ?? []) as Post[];
  }
  async remove(id: string): Promise<void> {
    const { DeleteCommand } = require('@aws-sdk/lib-dynamodb');
    await this.client().send(new DeleteCommand({ TableName: this.table, Key: { id } }));
  }
}

/** Provider binding chosen by RT_PROVIDER. */
export const PostRepositoryProvider = {
  provide: POST_REPOSITORY,
  useClass: PROVIDER === 'aws' ? DynamoPostRepository : InMemoryPostRepository,
};
