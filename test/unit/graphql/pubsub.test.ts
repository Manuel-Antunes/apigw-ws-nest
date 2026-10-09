import { describe, expect, it, vi } from 'vitest';
import {
  ApiGwPubSub,
  MessageType,
  PUBSUB_CONTEXT_KEY,
  apiGwPubSubContext,
  isClientMessage,
} from '../../../src/graphql';

describe('isClientMessage', () => {
  it('accepts what a graphql-ws client sends', () => {
    expect(isClientMessage({ type: MessageType.ConnectionInit })).toBe(true);
    expect(isClientMessage({ type: MessageType.Ping })).toBe(true);
    expect(isClientMessage({ type: MessageType.Pong })).toBe(true);
    expect(isClientMessage({ type: MessageType.Complete, id: '1' })).toBe(true);
    expect(isClientMessage({ type: MessageType.Subscribe, id: '1', payload: { query: '{ a }' } })).toBe(true);
  });

  it('leaves server-sent types, malformed messages and { event, data } frames alone', () => {
    expect(isClientMessage({ type: MessageType.ConnectionAck })).toBe(false);
    expect(isClientMessage({ type: MessageType.Next, id: '1', payload: {} })).toBe(false);
    expect(isClientMessage({ type: MessageType.Subscribe, id: '1' })).toBe(false);
    expect(isClientMessage({ event: 'chat.send', data: {} })).toBe(false);
    expect(isClientMessage(null)).toBe(false);
    expect(isClientMessage('connection_init')).toBe(false);
  });
});

describe('ApiGwPubSub', () => {
  const next = (iterable: unknown) => (iterable as AsyncIterator<unknown>).next();

  it('refuses to hand out a stream outside a subscription dispatch', () => {
    expect(() => new ApiGwPubSub().asyncIterableIterator('T')).toThrow(/outside a WebSocket subscription dispatch/);
  });

  it('captures the topics a resolver subscribes to, with a stream that never yields', async () => {
    const pubsub = new ApiGwPubSub();
    let stream: unknown;
    const { result, topics } = await pubsub.captureTopics(async () => {
      stream = pubsub.asyncIterableIterator(['A', 'B']);
      pubsub.asyncIterator('C');
      return 'source';
    });
    expect(result).toBe('source');
    expect(topics).toEqual(['A', 'B', 'C']);
    const pending = await Promise.race([next(stream), new Promise(r => setTimeout(() => r('pending'), 10))]);
    expect(pending).toEqual({ value: undefined, done: true });
  });

  it('replays exactly one payload', async () => {
    const pubsub = new ApiGwPubSub();
    const stream = await pubsub.replayPayload({ n: 1 }, async () => pubsub.asyncIterableIterator('T'));
    expect(await next(stream)).toEqual({ value: { n: 1 }, done: false });
    expect(await next(stream)).toEqual({ value: undefined, done: true });
  });

  it('keeps the mode per instance', async () => {
    const [a, b] = [new ApiGwPubSub(), new ApiGwPubSub()];
    await a.captureTopics(async () => {
      expect(() => b.asyncIterableIterator('T')).toThrow();
    });
  });

  it('refuses the in-process callback form of subscribe(), forwards the stream form', async () => {
    const pubsub = new ApiGwPubSub<any>();
    expect(() => pubsub.subscribe('T', () => {})).toThrow(/does not support PubSubEngine's subscribe/);
    const { topics } = await pubsub.captureTopics(async () => (pubsub as any).subscribe('T'));
    expect(topics).toEqual(['T']);
  });

  it('publishes a topic broadcast through the attached bus', async () => {
    const pubsub = new ApiGwPubSub();
    expect(pubsub.attached).toBe(false);
    await expect(pubsub.publish('T', 1)).rejects.toThrow(/not attached to a bridge/);
    const publish = vi.fn(async () => {});
    pubsub.attach({ publish });
    expect(pubsub.attached).toBe(true);
    await pubsub.publish('T', { n: 1 });
    expect(publish).toHaveBeenCalledWith({ kind: 'topic', topic: 'T', payload: { n: 1 } });
    pubsub.unsubscribe(); // nothing in-process to cancel
  });
});

describe('apiGwPubSubContext', () => {
  const pubsub = new ApiGwPubSub();

  it('publishes the PubSub on its own', async () => {
    expect(await apiGwPubSubContext(pubsub)()).toEqual({ [PUBSUB_CONTEXT_KEY]: pubsub });
  });

  it('merges a sync or async context factory, the PubSub winning a clash', async () => {
    const sync = apiGwPubSubContext(pubsub, ({ req }: any) => ({ user: req.user, pubsub: 'mine' }));
    expect(await sync({ req: { user: 'ada' } })).toEqual({ user: 'ada', pubsub });
    const asyncFactory = apiGwPubSubContext(pubsub, async () => ({ tenant: 't' }));
    expect(await asyncFactory()).toEqual({ tenant: 't', pubsub });
  });
});
