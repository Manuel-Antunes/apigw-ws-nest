/* =============================================================================
 *  End-to-end check: GraphQL subscriptions over API Gateway WebSocket.
 * =============================================================================
 *  Driven by the REAL `graphql-ws` browser/node client, not a hand-rolled
 *  imitation — if this client is satisfied, the server genuinely speaks
 *  graphql-transport-ws, subprotocol handshake included.
 *
 *    pnpm test:gql                 # against the local emulator (pnpm dev)
 *    pnpm test:gql:aws  + GQL_URL  # against a deployed API (pnpm live / deploy)
 *
 *  The local run additionally hits /__reload, which throws the Nest app and the
 *  bridge away mid-session — the closest thing to a redeploy. That case is the
 *  whole point: an in-process PubSub passes every other assertion here and fails
 *  that one.
 *
 *  LAG is how long to wait for a frame to round-trip. Locally a few hundred ms;
 *  against `sst dev` every frame is a separate invocation proxied to your machine
 *  through IoT, so budget seconds — a too-short LAG reads as "the first event
 *  never arrived" when the subscription simply hadn't been persisted yet.
 * ========================================================================== */

import { createClient } from 'graphql-ws';
import WebSocket from 'ws';

const URL = process.env.GQL_URL ?? 'ws://localhost:6005';
const RELOAD = process.env.RELOAD_URL ?? 'http://localhost:6005/__reload';
const LAG = Number(process.env.LAG ?? 400);

const results = [];
const check = (name, ok, detail = '') => {
  results.push({ name, ok });
  console.log(`${ok ? '  PASS' : '  FAIL'}  ${name}${detail ? `  — ${detail}` : ''}`);
};
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/** Tags every post this run creates, so concurrent runs stay isolated. */
const RUN = Math.random().toString(36).slice(2, 8);
const titled = (s) => `${s} ${RUN}`;
const feedName = (s) => `${s}-${RUN}`;

function makeClient(label) {
  let negotiated = null;
  const client = createClient({
    url: URL,
    webSocketImpl: WebSocket,
    lazy: false,
    retryAttempts: 0,
    on: {
      opened: (s) => { negotiated = s.protocol; },
      error: (e) => console.log(`  [${label}] client error`, e?.message ?? e),
    },
  });
  return { client, protocol: () => negotiated };
}

/**
 * Collect emissions belonging to THIS run.
 *
 * `postAdded` is a global topic, so a second run of this suite against the same
 * deployment legitimately reaches our subscriber too. Tagging every post with a
 * per-run id and ignoring everything else keeps concurrent runs from reading as
 * duplicate deliveries — which looks exactly like a fan-out bug and isn't one.
 */
function collect(client, query, variables) {
  const got = [];
  const errors = [];
  const dispose = client.subscribe({ query, variables }, {
    next: (v) => { if (JSON.stringify(v.data ?? {}).includes(RUN)) got.push(v); },
    error: (e) => errors.push(e),
    complete: () => {},
  });
  return { got, errors, dispose };
}

/** Run a single-result op (query/mutation) over the socket. */
function once(client, query, variables) {
  return new Promise((resolve, reject) => {
    let value;
    client.subscribe({ query, variables }, {
      next: (v) => (value = v),
      error: reject,
      complete: () => resolve(value),
    });
  });
}

const POST = '{ id title body createdAt }';

async function main() {
  console.log(`\n== graphql-ws over ${URL} ==\n`);

  /* ---- 1. handshake ---------------------------------------------------- */
  const a = makeClient('A');
  const b = makeClient('B');
  // force connection_init/ack by running a trivial query
  const health = await once(a.client, `query { posts ${POST} }`);
  check('query over websocket returns data', Array.isArray(health?.data?.posts),
        JSON.stringify(health?.data ?? health?.errors));
  check('subprotocol negotiated as graphql-transport-ws',
        a.protocol() === 'graphql-transport-ws', `got "${a.protocol()}"`);

  /* ---- 2. three subscription flavours ---------------------------------- */
  const all = collect(a.client, `subscription { postAdded ${POST} }`);
  const feed = collect(a.client,
    `subscription P($feed:String!){ postAddedIn(feed:$feed) ${POST} }`, { feed: feedName('general') });
  const term = collect(a.client,
    `subscription P($term:String!){ postAddedMatching(term:$term) ${POST} }`, { term: 'hello' });
  await sleep(LAG); // let the subscribe frames round-trip and persist

  /* ---- 3. a matching post from ANOTHER connection ---------------------- */
  const created = await once(b.client,
    `mutation C($t:String!,$b:String!,$f:String){ createPost(title:$t, body:$b, feed:$f) ${POST} }`,
    { t: titled('hello world'), b: 'body one', f: feedName('general') });
  check('mutation over websocket returns the created post',
        created?.data?.createPost?.title === titled('hello world'),
        JSON.stringify(created?.data ?? created?.errors));
  await sleep(LAG);

  check('global topic delivered (postAdded)', all.got.length === 1,
        `${all.got.length} emission(s)`);
  check('dynamic topic delivered (postAddedIn feed=general)', feed.got.length === 1,
        `${feed.got.length} emission(s)`);
  check('filtered subscription delivered (title contains "hello")', term.got.length === 1,
        `${term.got.length} emission(s)`);
  check('payload resolved through the `resolve` option',
        all.got[0]?.data?.postAdded?.title === titled('hello world'),
        JSON.stringify(all.got[0]?.data));

  /* ---- 4. a NON-matching post ------------------------------------------ */
  await once(b.client,
    `mutation C($t:String!,$b:String!,$f:String){ createPost(title:$t, body:$b, feed:$f) ${POST} }`,
    { t: titled('zebra report'), b: 'body two', f: feedName('other') });
  await sleep(LAG);

  check('global topic still delivers everything', all.got.length === 2,
        `${all.got.length} emission(s)`);
  check('dynamic topic NOT woken for another feed', feed.got.length === 1,
        `${feed.got.length} emission(s)`);
  check('filter rejected the non-matching title', term.got.length === 1,
        `${term.got.length} emission(s)`);

  /* ---- 5. cross-instance: rebuild the Nest app + bridge ---------------- */
  if (!process.env.SKIP_RELOAD) {
    const reload = await fetch(RELOAD, { method: 'POST' });
    const reloaded = await reload.json();
    check('emulator rebuilt the instance (new "Lambda")', reloaded.ok === true,
          JSON.stringify(reloaded));
    await sleep(LAG);
  }

  await once(b.client,
    `mutation C($t:String!,$b:String!,$f:String){ createPost(title:$t, body:$b, feed:$f) ${POST} }`,
    { t: titled('hello after redeploy'), b: 'body three', f: feedName('general') });
  await sleep(LAG);

  check('subscription SURVIVED a third publish (postAdded)', all.got.length === 3,
        `${all.got.length} emission(s)`);
  check('dynamic topic survived a third publish', feed.got.length === 2,
        `${feed.got.length} emission(s)`);
  check('filter survived a third publish', term.got.length === 2,
        `${term.got.length} emission(s)`);

  /* ---- 6. client-side complete stops delivery -------------------------- */
  all.dispose();
  await sleep(LAG);
  await once(b.client,
    `mutation C($t:String!,$b:String!){ createPost(title:$t, body:$b) ${POST} }`,
    { t: titled('hello again'), b: 'body four' });
  await sleep(LAG);
  check('completed subscription stops receiving', all.got.length === 3,
        `${all.got.length} emission(s)`);
  check('remaining subscription still receives', term.got.length === 3,
        `${term.got.length} emission(s)`);

  /* ---- 7. validation errors ------------------------------------------- */
  const bad = await new Promise((resolve) => {
    a.client.subscribe({ query: `subscription { nope { id } }` }, {
      next: () => {}, error: (e) => resolve(e), complete: () => resolve('completed'),
    });
  });
  check('invalid document answered with an `error` frame',
        Array.isArray(bad) && /nope/.test(JSON.stringify(bad)), JSON.stringify(bad));

  /* ---- 8. teardown ----------------------------------------------------- */
  a.client.dispose();
  b.client.dispose();
  await sleep(LAG);

  const failed = results.filter((r) => !r.ok);
  console.log(`\n${results.length - failed.length}/${results.length} passed\n`);
  process.exit(failed.length ? 1 : 0);
}

main().catch((err) => { console.error(err); process.exit(1); });
