/* GraphQL subscriptions inside a real Nest app: a code-first GraphQLModule,
 * wired by enableGraphQLSubscriptions, driven through the bridge. */

import { ApolloDriver, ApolloDriverConfig } from '@nestjs/apollo';
import { INestApplication, Module, UnauthorizedException } from '@nestjs/common';
import { Args, Context, Field, GraphQLModule, Mutation, ObjectType, Query, Resolver, Subscription } from '@nestjs/graphql';
import { OnGatewayConnection, WebSocketGateway } from '@nestjs/websockets';
import { PubSub } from 'graphql-subscriptions';
import { afterEach, describe, expect, it } from 'vitest';
import { GatewayBridge, GatewayClient, createNestApp } from '../../src';
import { ApiGwPubSub, apiGwPubSubContext, enableGraphQLSubscriptions } from '../../src/graphql';
import { connectEvent, frameEvent, nextId } from '../helpers/events';
import { SpyPublisher } from '../helpers/fakes';

@ObjectType()
class Note {
  @Field(() => String) text!: string;
  @Field(() => String, { nullable: true }) by?: string | null;
}

@Resolver(() => Note)
class NoteResolver {
  @Query(() => String, { nullable: true })
  whoami(@Context('connection') connection?: { data?: { user?: string } }) {
    return connection?.data?.user ?? null;
  }

  @Mutation(() => Note)
  async note(@Context('pubsub') pubsub: PubSub, @Args('text', { type: () => String }) text: string) {
    await pubsub.publish('NOTED', { text });
    return { text };
  }

  @Subscription(() => Note, {
    resolve: (payload: { text: string }, _args: unknown, ctx: any) => ({
      text: payload.text,
      by: ctx.connection?.data?.user ?? null,
    }),
    filter: (payload: { text: string }, vars: { term?: string }) => !vars.term || payload.text.includes(vars.term),
  })
  noted(@Context('pubsub') pubsub: PubSub, @Args('term', { type: () => String, nullable: true }) _term?: string) {
    return pubsub.asyncIterableIterator('NOTED');
  }
}

@WebSocketGateway()
class IdentityGateway implements OnGatewayConnection {
  handleConnection(client: GatewayClient<{ user?: string }>) {
    const token = client.handshake.query.token;
    if (token === 'bad') throw new UnauthorizedException();
    if (token) client.data.user = token;
  }
}

const pubsub = new ApiGwPubSub();

@Module({
  imports: [
    GraphQLModule.forRoot<ApolloDriverConfig>({
      driver: ApolloDriver,
      autoSchemaFile: true,
      context: apiGwPubSubContext(pubsub),
    }),
  ],
  providers: [NoteResolver, IdentityGateway],
})
class GqlModule {}

@Module({})
class NoGraphQLModule {}

let app: INestApplication | undefined;
afterEach(async () => {
  await app?.close();
  app = undefined;
});

async function start() {
  const publisher = new SpyPublisher();
  const bridge = GatewayBridge.builder().provider('local').publisher(publisher).build();
  app = await createNestApp(GqlModule, bridge, { nest: { logger: false } });
  await app.init();
  const returned = enableGraphQLSubscriptions(app, bridge, { pubsub });
  /** Open a graphql-transport-ws connection, optionally with ?token=. */
  const open = async (token?: string) => {
    const id = nextId();
    const res = await bridge.dispatch(
      connectEvent(id, {
        headers: { 'Sec-WebSocket-Protocol': 'graphql-transport-ws' },
        queryStringParameters: token ? { token } : {},
      }),
    );
    if (res.statusCode === 200) await bridge.dispatch(frameEvent(id, { type: 'connection_init' }));
    return { id, res };
  };
  const send = (id: string, frame: unknown) => bridge.dispatch(frameEvent(id, frame));
  const frames = (id: string) => publisher.raw.filter(m => m.id === id).map(m => m.payload);
  return { bridge, publisher, returned, open, send, frames };
}

describe('enableGraphQLSubscriptions in a real app', () => {
  it('registers the protocol, returning the PubSub it was given', async () => {
    const { bridge, returned } = await start();
    expect(returned).toBe(pubsub);
    expect(pubsub.attached).toBe(true);
    expect(bridge.subprotocols).toEqual(['graphql-transport-ws']);
  });

  it('negotiates graphql-transport-ws and acknowledges connection_init', async () => {
    const { open, frames } = await start();
    const { id, res } = await open();
    expect(res.headers).toEqual({ 'Sec-WebSocket-Protocol': 'graphql-transport-ws' });
    expect(frames(id)).toEqual([{ type: 'connection_ack' }]);
  });

  it('refuses a socket handleConnection refuses — GraphQL sockets go through $connect too', async () => {
    const { open } = await start();
    expect((await open('bad')).res.statusCode).toBe(401);
  });

  it('answers whoami from the identity established at $connect', async () => {
    const { open, send, frames } = await start();
    const bob = await open('bob');
    const anon = await open();
    await send(bob.id, { type: 'subscribe', id: 'q', payload: { query: '{ whoami }' } });
    await send(anon.id, { type: 'subscribe', id: 'q', payload: { query: '{ whoami }' } });
    expect(frames(bob.id)[1]).toEqual({ type: 'next', id: 'q', payload: { data: { whoami: 'bob' } } });
    expect(frames(anon.id)[1]).toEqual({ type: 'next', id: 'q', payload: { data: { whoami: null } } });
  });

  it('delivers a subscription, filtered per subscriber, with the subscriber\'s identity', async () => {
    const { open, send, frames } = await start();
    const ada = await open('ada');
    const picky = await open('picky');
    await send(ada.id, { type: 'subscribe', id: 's', payload: { query: 'subscription { noted { text by } }' } });
    await send(picky.id, {
      type: 'subscribe', id: 's',
      payload: { query: 'subscription($t: String) { noted(term: $t) { text } }', variables: { t: 'cat' } },
    });
    const author = await open();
    await send(author.id, { type: 'subscribe', id: 'm', payload: { query: 'mutation { note(text: "a dog") { text } }' } });
    await send(author.id, { type: 'subscribe', id: 'm2', payload: { query: 'mutation { note(text: "a cat") { text } }' } });
    const nexts = (id: string) => frames(id).filter(f => f.type === 'next').map(f => f.payload.data.noted);
    expect(nexts(ada.id)).toEqual([{ text: 'a dog', by: 'ada' }, { text: 'a cat', by: 'ada' }]);
    expect(nexts(picky.id)).toEqual([{ text: 'a cat' }]);
  });

  it('survives a new instance: same store and registry, new app and bridge', async () => {
    const first = await start();
    const ada = await first.open('ada');
    await first.send(ada.id, { type: 'subscribe', id: 's', payload: { query: 'subscription { noted { text by } }' } });
    const registry = (first.bridge as any).protocols.find((p: any) => p.name === 'graphql-ws')._registry;

    const publisher = new SpyPublisher();
    const bridge = GatewayBridge.builder().provider('local').store(first.bridge.store).publisher(publisher).build();
    const second = await createNestApp(GqlModule, bridge, { nest: { logger: false } });
    await second.init();
    try {
      enableGraphQLSubscriptions(second, bridge, { pubsub, registry });
      await bridge.dispatch(frameEvent(ada.id, {
        type: 'subscribe', id: 'm', payload: { query: 'mutation { note(text: "after redeploy") { text } }' },
      }));
      const next = publisher.raw.find(m => m.id === ada.id && m.payload.id === 's');
      expect(next?.payload.payload.data.noted).toEqual({ text: 'after redeploy', by: 'ada' });
    } finally {
      await second.close();
    }
  });

  it('explains a call made before init()', async () => {
    const bridge = GatewayBridge.builder().provider('local').build();
    app = await createNestApp(GqlModule, bridge, { nest: { logger: false } });
    expect(() => enableGraphQLSubscriptions(app!, bridge)).toThrow(/Call this AFTER/);
  });

  it('explains an app without GraphQLModule', async () => {
    const bridge = GatewayBridge.builder().provider('local').build();
    app = await createNestApp(NoGraphQLModule, bridge, { nest: { logger: false } });
    await app.init();
    expect(() => enableGraphQLSubscriptions(app!, bridge)).toThrow(/GraphQLSchemaHost is not available/);
  });
});
