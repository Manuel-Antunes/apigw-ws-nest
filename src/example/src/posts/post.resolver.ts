/* =============================================================================
 *  PostResolver — a completely ordinary NestJS GraphQL resolver.
 * =============================================================================
 *  Nothing here knows about API Gateway, Lambda, DynamoDB or connection ids. It
 *  is the standard @Query / @Mutation / @Subscription surface, with the standard
 *  `resolve` and `filter` options. The ONE difference from a resolver you'd write
 *  for a normal Nest server is which PubSub is injected: ApiGwPubSub instead of
 *  graphql-subscriptions' in-process PubSub — which cannot survive a frozen
 *  container, let alone reach a subscriber on another one. Same call shape.
 *
 *  The three subscriptions exist to prove three different things:
 *
 *    postAdded          GLOBAL topic. Every subscriber is woken by every post.
 *    postAddedIn(feed)  DYNAMIC topic, computed from the subscriber's argument.
 *                       Only that feed's subscribers are looked up at all — the
 *                       precise-fan-out pattern you actually want serverless,
 *                       since a topic maps to one DynamoDB partition.
 *    postAddedMatching  GLOBAL topic + a server-side `filter`. Proves the filter
 *      (term)           really runs at publish time, per subscriber, with that
 *                       subscriber's own variables — even though nothing was
 *                       held in memory between subscribe and publish.
 * ========================================================================== */

import { Inject } from '@nestjs/common';
import { Args, Context, ID, Mutation, Query, Resolver, Subscription } from '@nestjs/graphql';
import { PubSub } from 'graphql-subscriptions';
import { PUBSUB_CONTEXT_KEY as PUBSUB } from '../../../graphql';
import { PostModel } from './post.model';
import { Post } from './post.repository';
import { PostService } from './post.service';

/** Every new post lands here. */
const POST_ADDED = 'POST_ADDED';
/** ...and here, if it was addressed to a feed. One partition per feed. */
const feedTopic = (feed: string) => `POST_ADDED#${feed}`;

/** Payload shape shared by every publish below; the `resolve` options unwrap it. */
interface PostPayload {
  post: Post;
}

@Resolver(() => PostModel)
export class PostResolver {
  // Explicit @Inject + explicit @Args types throughout: esbuild (which SST uses
  // to bundle Lambdas) does not implement emitDecoratorMetadata, so anything
  // that depends on `design:type`/`design:paramtypes` is undefined at runtime.
  //
  // NOTE what is NOT injected here: the PubSub. It arrives on the GraphQL
  // context (@Context('pubsub')), so this resolver names no transport at all —
  // the same file runs against an in-process PubSub on a long-lived server.
  constructor(@Inject(PostService) private readonly posts: PostService) {}

  @Query(() => [PostModel], { name: 'posts', description: 'The whole feed.' })
  list(): Promise<Post[]> {
    return this.posts.list();
  }

  /** graphql-ws runs mutations over the SAME socket as subscriptions, so the test
   *  client can trigger a fan-out without any HTTP endpoint at all. */
  @Mutation(() => PostModel)
  async createPost(
    @Context(PUBSUB) pubsub: PubSub,
    @Args('title', { type: () => String }) title: string,
    @Args('body', { type: () => String }) body: string,
    @Args('feed', { type: () => String, nullable: true }) feed?: string,
  ): Promise<Post> {
    const post = await this.posts.create({ title, body });
    // Awaited: on Lambda the container freezes the instant the handler returns,
    // so the pushes must land before this resolver resolves.
    await pubsub.publish(POST_ADDED, { post } satisfies PostPayload);
    if (feed) await pubsub.publish(feedTopic(feed), { post } satisfies PostPayload);
    return post;
  }

  @Mutation(() => Boolean)
  async deletePost(@Args('id', { type: () => ID }) id: string): Promise<boolean> {
    await this.posts.remove(id);
    return true;
  }

  @Subscription(() => PostModel, {
    description: 'Every new post, from anywhere.',
    resolve: (payload: PostPayload) => payload.post,
  })
  postAdded(@Context(PUBSUB) pubsub: PubSub) {
    return pubsub.asyncIterableIterator(POST_ADDED);
  }

  @Subscription(() => PostModel, {
    description: 'New posts addressed to one feed — the topic is per-subscriber.',
    resolve: (payload: PostPayload) => payload.post,
  })
  postAddedIn(
    @Context(PUBSUB) pubsub: PubSub,
    @Args('feed', { type: () => String }) feed: string,
  ) {
    return pubsub.asyncIterableIterator(feedTopic(feed));
  }

  @Subscription(() => PostModel, {
    description: 'New posts whose title contains `term` (server-side filter).',
    resolve: (payload: PostPayload) => payload.post,
    filter: (payload: PostPayload, variables: { term: string }) =>
      payload.post.title.toLowerCase().includes(String(variables.term).toLowerCase()),
  })
  postAddedMatching(
    @Context(PUBSUB) pubsub: PubSub,
    @Args('term', { type: () => String }) term: string,
  ) {
    return pubsub.asyncIterableIterator(POST_ADDED);
  }
}
