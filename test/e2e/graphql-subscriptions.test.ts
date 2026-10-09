/* =============================================================================
 *  GraphQL subscriptions over the API Gateway WebSocket — end to end.
 * =============================================================================
 *  Driven by the REAL graphql-ws client: if it is satisfied, the server
 *  genuinely speaks graphql-transport-ws, subprotocol handshake included.
 *
 *  Against the emulator the suite also rebuilds the Nest app and the bridge
 *  mid-session (/__reload) — the closest thing to a redeploy. That step is the
 *  point: an in-process PubSub passes every other assertion here and fails it.
 * ========================================================================== */

import { Client, createClient } from 'graphql-ws';
import WebSocket from 'ws';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { LAG, REMOTE, Target, reload, sleep, startTarget } from './target';

/** Tags everything this run creates, so concurrent runs stay apart. */
const RUN = Math.random().toString(36).slice(2, 8);
const titled = (s: string) => `${s} ${RUN}`;
const feed = (s: string) => `${s}-${RUN}`;
const POST = '{ id title body createdAt }';

let target: Target;
const clients: Client[] = [];

function client(query = '') {
  let protocol: string | undefined;
  const c = createClient({
    url: `${target.wsUrl}${query}`,
    webSocketImpl: WebSocket,
    lazy: false,
    retryAttempts: 0,
    on: { opened: socket => void (protocol = (socket as WebSocket).protocol) },
  });
  clients.push(c);
  return { client: c, protocol: () => protocol };
}

/** Run a single-result operation (query or mutation) over the socket. */
function once(c: Client, query: string, variables?: Record<string, unknown>): Promise<any> {
  return new Promise((resolve, reject) => {
    let value: unknown;
    c.subscribe({ query, variables }, { next: v => (value = v), error: reject, complete: () => resolve(value) });
  });
}

/** Collect this run's emissions of a subscription. */
function collect(c: Client, query: string, variables?: Record<string, unknown>) {
  const got: any[] = [];
  const dispose = c.subscribe(
    { query, variables },
    { next: v => void (JSON.stringify(v.data ?? {}).includes(RUN) && got.push(v)), error: () => {}, complete: () => {} },
  );
  return { got, dispose };
}

const createPost = (c: Client, title: string, body: string, feedName?: string) =>
  once(
    c,
    `mutation C($t:String!,$b:String!,$f:String){ createPost(title:$t, body:$b, feed:$f) ${POST} }`,
    { t: titled(title), b: body, f: feedName },
  );

beforeAll(async () => {
  target = await startTarget();
});
afterAll(async () => {
  for (const c of clients) await c.dispose();
  await sleep(50);
  await target?.close();
});

describe('graphql-ws over the API Gateway socket', () => {
  let a: ReturnType<typeof client>;
  let b: ReturnType<typeof client>;
  let all: ReturnType<typeof collect>;
  let inFeed: ReturnType<typeof collect>;
  let matching: ReturnType<typeof collect>;

  it('answers a query, over a negotiated graphql-transport-ws', async () => {
    a = client();
    b = client();
    const result = await once(a.client, `query { posts ${POST} }`);
    expect(Array.isArray(result.data.posts)).toBe(true);
    expect(a.protocol()).toBe('graphql-transport-ws');
  });

  it('delivers to a global topic, a dynamic topic and a filtered subscription', async () => {
    all = collect(a.client, `subscription { postAdded ${POST} }`);
    inFeed = collect(a.client, `subscription P($feed:String!){ postAddedIn(feed:$feed) ${POST} }`, { feed: feed('general') });
    matching = collect(a.client, `subscription P($term:String!){ postAddedMatching(term:$term) ${POST} }`, { term: 'hello' });
    await sleep(LAG); // let the subscribe frames be persisted

    const created = await createPost(b.client, 'hello world', 'body one', feed('general'));
    expect(created.data.createPost.title).toBe(titled('hello world'));

    await expect.poll(() => all.got.length, { timeout: LAG * 10 }).toBe(1);
    await expect.poll(() => inFeed.got.length, { timeout: LAG * 10 }).toBe(1);
    await expect.poll(() => matching.got.length, { timeout: LAG * 10 }).toBe(1);
    expect(all.got[0].data.postAdded.title).toBe(titled('hello world'));
  });

  it('wakes only the right feed, and lets the filter reject', async () => {
    await createPost(b.client, 'zebra report', 'body two', feed('other'));
    await expect.poll(() => all.got.length, { timeout: LAG * 10 }).toBe(2);
    await sleep(LAG);
    expect(inFeed.got).toHaveLength(1);
    expect(matching.got).toHaveLength(1);
  });

  it.skipIf(REMOTE)('survives a new instance (new app, new bridge, same durable state)', async () => {
    expect(await reload(target)).toBeGreaterThan(1);
    await sleep(LAG);
    await createPost(b.client, 'hello after redeploy', 'body three', feed('general'));
    await expect.poll(() => all.got.length, { timeout: LAG * 10 }).toBe(3);
    await expect.poll(() => inFeed.got.length, { timeout: LAG * 10 }).toBe(2);
    await expect.poll(() => matching.got.length, { timeout: LAG * 10 }).toBe(2);
  });

  it('stops delivering a completed subscription', async () => {
    const before = all.got.length;
    all.dispose();
    await sleep(LAG);
    await createPost(b.client, 'hello again', 'body four');
    await expect.poll(() => matching.got.length, { timeout: LAG * 10 }).toBe(before);
    await sleep(LAG);
    expect(all.got).toHaveLength(before);
  });

  it('answers an invalid document with an error', async () => {
    const error = await new Promise(resolve =>
      a.client.subscribe({ query: 'subscription { nope { id } }' }, { next: () => {}, error: resolve, complete: () => resolve('completed') }),
    );
    expect(JSON.stringify(error)).toMatch(/nope/);
  });

  it('puts the identity from $connect in the GraphQL context', async () => {
    const bob = client(`?token=demo:bob-${RUN}`);
    expect((await once(bob.client, 'query { whoami }')).data.whoami).toBe(`bob-${RUN}`);
    expect((await once(a.client, 'query { whoami }')).data.whoami).toBeNull();
  });
});
