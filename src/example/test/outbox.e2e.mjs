/* =============================================================================
 *  Outbox guarantees — the properties the graphql-ws suite cannot observe.
 * =============================================================================
 *  The end-to-end suite proves messages ARRIVE. It cannot prove what happens when
 *  a delivery FAILS, because it has no way to make one fail. These are exactly
 *  the properties the outbox was built for, so they get asserted directly against
 *  the built library — through the PUBLIC surface, so this doubles as a check
 *  that the builder + ProtocolHandler contract actually compose:
 *
 *    - a failed delivery propagates, so Lambda redelivers the stream record
 *    - the redelivery does NOT re-send to subscribers already served
 *    - ...but DOES re-send to the one that failed (its claim was released)
 *    - a 410 Gone is reaped through the protocol's onDisconnect, not retried
 *    - subscribers are paged, and running out of time requeues the REMAINDER
 *      under the same messageId rather than losing it
 *    - bridge.flush keeps per-topic order and reports per-record failures
 *
 *      pnpm build && node src/example/test/outbox.e2e.mjs
 * ========================================================================== */

import { GatewayBridge, ConnectionGoneError } from '../../../dist/index.mjs';

const results = [];
const check = (name, ok, detail = '') => {
  results.push({ name, ok });
  console.log(`${ok ? '  PASS' : '  FAIL'}  ${name}${detail ? `  — ${detail}` : ''}`);
};

/** A record as the bus would seal it. */
const record = (over = {}) => ({
  kind: 'test', topic: 't', payload: { n: 1 },
  messageId: 'msg-1', publishedAt: Date.now(), ...over,
});

/**
 * A bridge carrying one synthetic protocol whose fan-out is a fixed list of
 * subscriber ids. Ids in `failing` throw; ids in `gone` raise 410.
 */
function harness({ ids, failing = new Set(), gone = new Set(), pageSize = 100 }) {
  const sent = [];
  const requeued = [];
  const reaped = [];

  const bridge = GatewayBridge.builder()
    .provider('local')
    // Capture continuations instead of delivering them, so the requeue is
    // observable rather than immediately consumed by the inline bus.
    .bus({ publish: async (r) => { requeued.push(r); } })
    .concurrency(4)
    .use({
      name: 'test-protocol',
      handleFrame: async () => false,
      onDisconnect: async (id) => { reaped.push(id); },
      fanout: {
        test: async (_rec, cursor) => {
          const start = cursor?.at ?? 0;
          const slice = ids.slice(start, start + pageSize);
          const next = start + pageSize;
          return {
            targets: slice.map((id) => ({
              connectionId: id,
              key: `CONN#${id}`,
              send: async () => {
                if (gone.has(id)) throw new ConnectionGoneError(id);
                if (failing.has(id)) throw new Error(`boom ${id}`);
                sent.push(id);
              },
            })),
            cursor: next < ids.length ? { at: next } : undefined,
          };
        },
      },
    })
    .build();

  return { bridge, sent, requeued, reaped };
}

async function main() {
  console.log('\n== outbox delivery guarantees ==\n');

  /* ---- 0. the builder --------------------------------------------------- */
  {
    const custom = { add: async () => {}, remove: async () => {}, join: async () => {},
                     leave: async () => {}, membersOf: async () => ['x'] };
    const bridge = GatewayBridge.builder().provider('aws').store(custom).build();
    check('an individual setter overrides the provider preset',
          bridge.store === custom && bridge.provider === 'aws');
    check('room fan-out is built in, with no protocol registered',
          bridge.flusher.kinds().includes('room'), bridge.flusher.kinds().join(','));
    check('two bridges share nothing',
          GatewayBridge.builder().provider('local').build().store !==
          GatewayBridge.builder().provider('local').build().store);
    const withProtocol = GatewayBridge.builder().provider('local')
      .use({ subprotocol: 'graphql-transport-ws', handleFrame: async () => false }).build();
    check('a registered protocol makes its subprotocol negotiable',
          withProtocol.subprotocols.includes('graphql-transport-ws'),
          withProtocol.subprotocols.join(','));
    check('...and an unregistered one is NOT offered',
          !GatewayBridge.builder().provider('local').build()
            .subprotocols.includes('graphql-transport-ws'));
  }

  /* ---- 1. batching: one flush serves the whole topic -------------------- */
  {
    const ids = Array.from({ length: 50 }, (_, i) => `c${i}`);
    const { bridge, sent } = harness({ ids });
    await bridge.flushRecord(record());
    check('one flush delivers every subscriber on the topic',
          sent.length === 50, `${sent.length}/50 sent`);
  }

  /* ---- 2. a failure propagates (so the stream retries) ------------------ */
  {
    const { bridge, sent } = harness({ ids: ['a', 'b', 'c'], failing: new Set(['b']) });
    let threw = false;
    await bridge.flushRecord(record()).catch(() => { threw = true; });
    check('a failed delivery makes the whole record fail', threw);
    check('the healthy subscribers were still served',
          sent.sort().join(',') === 'a,c', sent.join(','));
  }

  /* ---- 3. the retry is idempotent -------------------------------------- */
  {
    const failing = new Set(['b']);
    const { bridge, sent } = harness({ ids: ['a', 'b', 'c'], failing });
    await bridge.flushRecord(record()).catch(() => {});
    failing.delete('b'); // the transient fault clears
    await bridge.flushRecord(record()); // same messageId — this is the redelivery

    check('redelivery does NOT duplicate for already-served subscribers',
          sent.filter((id) => id === 'a').length === 1,
          `a served ${sent.filter((i) => i === 'a').length}x`);
    check('redelivery DOES serve the one that failed', sent.includes('b'), sent.join(','));
    check('every subscriber ends up served exactly once',
          sent.sort().join(',') === 'a,b,c', sent.join(','));
  }

  /* ---- 4. a dead socket is reaped, not retried forever ------------------ */
  {
    const { bridge, sent, reaped } = harness({ ids: ['a', 'ghost'], gone: new Set(['ghost']) });
    let threw = false;
    await bridge.flushRecord(record()).catch(() => { threw = true; });
    check('410 Gone does not fail the record', !threw);
    check('410 Gone runs the protocol onDisconnect hook',
          reaped.join(',') === 'ghost', reaped.join(','));
    check('the live subscriber was still served', sent.join(',') === 'a', sent.join(','));
  }

  /* ---- 5. paging + continuation ---------------------------------------- */
  {
    const ids = Array.from({ length: 25 }, (_, i) => `c${i}`);
    const { bridge, sent, requeued } = harness({ ids, pageSize: 10 });
    await bridge.flushRecord(record());
    check('paging walks every page when there is time',
          sent.length === 25 && requeued.length === 0,
          `${sent.length} sent, ${requeued.length} requeued`);
  }
  {
    const ids = Array.from({ length: 25 }, (_, i) => `c${i}`);
    const { bridge, sent, requeued } = harness({ ids, pageSize: 10 });
    // Out of runway after the first page.
    await bridge.flushRecord(record(), { remainingMs: () => 1 });
    check('running out of time requeues the remainder instead of dropping it',
          sent.length === 10 && requeued.length === 1,
          `${sent.length} sent, ${requeued.length} requeued`);
    check('the continuation keeps the messageId (so dedupe still applies)',
          requeued[0]?.messageId === 'msg-1', requeued[0]?.messageId);
    check('the continuation carries the cursor',
          requeued[0]?.cursor?.at === 10, JSON.stringify(requeued[0]?.cursor));
  }

  /* ---- 6. an unknown kind fails loudly ---------------------------------- */
  {
    const { bridge } = harness({ ids: ['a'] });
    const err = await bridge.flushRecord(record({ kind: 'nope' })).catch((e) => e);
    check('an unregistered kind throws rather than silently dropping',
          /no fan-out resolver/.test(String(err?.message)), String(err?.message).slice(0, 55));
  }

  /* ---- 7. bridge.flush: ordering + per-record failure ------------------- */
  {
    const order = [];
    const failing = new Set(['t2-b']);
    const bridge = GatewayBridge.builder().provider('local')
      .bus({ publish: async () => {} })
      .use({
        name: 'test-protocol',
        handleFrame: async () => false,
        fanout: {
          test: async (rec) => ({
            targets: [{
              connectionId: 'c', key: `K#${rec.messageId}`,
              send: async () => {
                if (failing.has(rec.messageId)) throw new Error('boom');
                order.push(rec.messageId);
              },
            }],
          }),
        },
      })
      .build();

    // Two topics; the second topic's FIRST message fails.
    const image = (pk, messageId) => ({
      eventName: 'INSERT',
      dynamodb: {
        SequenceNumber: messageId,
        NewImage: {
          pk: { S: pk }, sk: { S: messageId }, kind: { S: 'test' },
          topic: { S: pk }, messageId: { S: messageId },
          publishedAt: { N: '1' }, payload: { M: {} },
        },
      },
    });
    const out = await bridge.flush({
      Records: [
        image('TOPIC#t1', 't1-a'), image('TOPIC#t1', 't1-b'),
        image('TOPIC#t2', 't2-b'), image('TOPIC#t2', 't2-c'),
      ],
    });

    check('records of one topic are delivered in order',
          order.indexOf('t1-a') < order.indexOf('t1-b'), order.join(','));
    check('a failed record is reported for redelivery',
          out.batchItemFailures.some((f) => f.itemIdentifier === 't2-b'),
          JSON.stringify(out.batchItemFailures));
    check('a failure stops LATER messages of the same topic (no reordering)',
          !order.includes('t2-c'), order.join(','));
    check('another topic is unaffected by that failure',
          order.includes('t1-a') && order.includes('t1-b'), order.join(','));
    const ignored = await bridge.flush({ Records: [{ eventName: 'REMOVE', dynamodb: {} }] });
    check('non-INSERT stream records are ignored',
          ignored.batchItemFailures.length === 0);
  }

  /* ---- 8. server.to().emit() records rather than delivers --------------- */
  {
    const published = [];
    const bridge = GatewayBridge.builder().provider('local')
      .bus({ publish: async (m) => { published.push(m); } })
      .build();
    await bridge.server.to('orders').emit('order.updated', { id: 7 });
    check('server.to(room).emit publishes a room broadcast, it does not send',
          published.length === 1 && published[0].kind === 'room' &&
          published[0].room === 'orders' && published[0].event === 'order.updated',
          JSON.stringify(published[0]));
    await bridge.server.emit('announcement', { text: 'hi' });
    check('server.emit targets the global room',
          published[1]?.room === '@@global', published[1]?.room);
  }

  const failed = results.filter((r) => !r.ok);
  console.log(`\n${results.length - failed.length}/${results.length} passed\n`);
  process.exit(failed.length ? 1 : 0);
}

main().catch((err) => { console.error(err); process.exit(1); });
