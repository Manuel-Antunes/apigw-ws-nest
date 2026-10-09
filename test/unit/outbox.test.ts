import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  ConnectionGoneError,
  DeliveryTarget,
  DynamoDeliveryLedger,
  DynamoOutboxBus,
  Flusher,
  InMemoryDeliveryLedger,
  InlineMessageBus,
  OUTBOX_TTL_SECONDS,
  OutboxRecord,
  decodeStreamEvent,
  outboxPk,
  sealRecord,
} from '../../src';
import { fakeDynamo } from '../helpers/fake-dynamo';

vi.mock('../../src/providers/dynamo', async importOriginal => {
  const { fakeDynamo } = await import('../helpers/fake-dynamo');
  return {
    ...(await importOriginal<typeof import('../../src/providers/dynamo')>()),
    docClient: (_key?: string, options?: any) => fakeDynamo.client(options),
  };
});

beforeEach(() => fakeDynamo.reset());
afterEach(() => {
  vi.unstubAllEnvs();
  delete process.env.MANAGEMENT_ENDPOINT;
});

const record = (over: Partial<OutboxRecord> & Record<string, unknown> = {}): OutboxRecord =>
  ({
    kind: 'test',
    topic: 't',
    payload: { n: 1 },
    messageId: 'msg-1',
    publishedAt: 1,
    ...over,
  }) as OutboxRecord;

describe('outboxPk', () => {
  it('partitions by room, topic, or kind', () => {
    expect(outboxPk({ kind: 'room', room: 'r1', event: 'e', data: null })).toBe('ROOM#r1');
    expect(outboxPk({ kind: 'topic', topic: 'POSTS', payload: null })).toBe('TOPIC#POSTS');
    expect(outboxPk({ kind: 'presence' })).toBe('TOPIC#presence');
  });
});

describe('sealRecord', () => {
  it('stamps identity and the endpoint the publish happened under', () => {
    process.env.MANAGEMENT_ENDPOINT = 'https://api.example/stage';
    const sealed = sealRecord({ kind: 'topic', topic: 'T', payload: 1 });
    expect(sealed.messageId).toMatch(/^[0-9a-f-]{36}$/);
    expect(sealed.publishedAt).toBeTypeOf('number');
    expect(sealed.endpoint).toBe('https://api.example/stage');
  });

  it('leaves the endpoint out when there is none', () => {
    expect(sealRecord({ kind: 'topic', topic: 'T', payload: 1 })).not.toHaveProperty('endpoint');
  });

  it('keeps an existing messageId — a retry or continuation must not get a new one', () => {
    const sealed = record();
    expect(sealRecord(sealed)).toBe(sealed);
  });
});

describe('InlineMessageBus', () => {
  it('delivers the sealed record right away', async () => {
    const deliver = vi.fn(async () => {});
    await new InlineMessageBus(deliver).publish({ kind: 'topic', topic: 'T', payload: 1 });
    expect(deliver).toHaveBeenCalledWith(expect.objectContaining({ kind: 'topic', messageId: expect.any(String) }));
  });
});

describe('DynamoOutboxBus', () => {
  it('writes one time-ordered row per publish, partitioned by topic', async () => {
    const now = Date.now();
    await new DynamoOutboxBus('outbox-table').publish({
      kind: 'room', room: 'lobby', event: 'hi', data: { optional: undefined },
    });
    const [row] = fakeDynamo.items('outbox-table');
    expect(row.pk).toBe('ROOM#lobby');
    expect(row.sk).toMatch(/^\d{13}#[0-9a-f-]{36}$/);
    expect(row).toMatchObject({ kind: 'room', room: 'lobby', event: 'hi' });
    expect(row.expiresAt).toBeGreaterThanOrEqual(Math.floor(now / 1000) + OUTBOX_TTL_SECONDS);
  });

  it('defaults the table from MESSAGES_TABLE', async () => {
    vi.stubEnv('MESSAGES_TABLE', 'from-env');
    await new DynamoOutboxBus().publish({ kind: 'topic', topic: 'T', payload: 1 });
    expect(fakeDynamo.items('from-env')).toHaveLength(1);
  });
});

describe.each([
  ['InMemoryDeliveryLedger', () => new InMemoryDeliveryLedger()],
  ['DynamoDeliveryLedger', () => new DynamoDeliveryLedger('ledger')],
])('%s', (_name, make) => {
  it('claims a delivery once', async () => {
    const ledger = make();
    expect(await ledger.claim('m1', 'k1')).toBe(true);
    expect(await ledger.claim('m1', 'k1')).toBe(false);
    expect(await ledger.claim('m1', 'k2')).toBe(true);
    expect(await ledger.claim('m2', 'k1')).toBe(true);
  });

  it('can be claimed again once released', async () => {
    const ledger = make();
    await ledger.claim('m1', 'k1');
    await ledger.release('m1', 'k1');
    expect(await ledger.claim('m1', 'k1')).toBe(true);
  });
});

describe('DynamoDeliveryLedger', () => {
  it('writes one partition per delivery, with a ttl', async () => {
    await new DynamoDeliveryLedger('ledger').claim('m1', 'CONN#a');
    expect(fakeDynamo.items('ledger')).toEqual([
      { pk: 'DLV#m1#CONN#a', sk: 'DLV', ttl: expect.any(Number) },
    ]);
  });

  it('propagates errors other than a failed condition', async () => {
    fakeDynamo.failWith = () => new Error('throttled');
    await expect(new DynamoDeliveryLedger('ledger').claim('m', 'k')).rejects.toThrow('throttled');
  });

  it('defaults the table from CONNECTIONS_TABLE', async () => {
    vi.stubEnv('CONNECTIONS_TABLE', 'conns');
    await new DynamoDeliveryLedger().claim('m', 'k');
    expect(fakeDynamo.items('conns')).toHaveLength(1);
  });
});

/* ---- the flusher --------------------------------------------------------- */

/** A flusher with one kind whose fan-out is a fixed, paged list of ids. */
function harness({
  ids,
  failing = new Set<string>(),
  gone = new Set<string>(),
  pageSize = 100,
  concurrency,
}: {
  ids: string[];
  failing?: Set<string>;
  gone?: Set<string>;
  pageSize?: number;
  concurrency?: number;
}) {
  const sent: string[] = [];
  const requeued: OutboxRecord[] = [];
  const reaped: string[] = [];
  let inFlight = 0;
  let maxInFlight = 0;
  const flusher = new Flusher({
    ledger: new InMemoryDeliveryLedger(),
    bus: () => ({ publish: async r => void requeued.push(r as OutboxRecord) }),
    onConnectionGone: async id => void reaped.push(id),
    concurrency,
  });
  flusher.register('test', async (_record, cursor) => {
    const start = (cursor?.at as number) ?? 0;
    const next = start + pageSize;
    return {
      targets: ids.slice(start, next).map(
        (id): DeliveryTarget => ({
          connectionId: id,
          key: `CONN#${id}`,
          send: async () => {
            inFlight += 1;
            maxInFlight = Math.max(maxInFlight, inFlight);
            await new Promise(resolve => setTimeout(resolve, 1));
            inFlight -= 1;
            if (gone.has(id)) throw new ConnectionGoneError(id);
            if (failing.has(id)) throw new Error(`boom ${id}`);
            sent.push(id);
          },
        }),
      ),
      cursor: next < ids.length ? { at: next } : undefined,
    };
  });
  return { flusher, sent, requeued, reaped, maxInFlight: () => maxInFlight };
}

describe('Flusher', () => {
  beforeEach(() => {
    vi.spyOn(console, 'error').mockImplementation(() => {});
  });

  it('serves a whole topic in one flush', async () => {
    const { flusher, sent } = harness({ ids: Array.from({ length: 50 }, (_, i) => `c${i}`) });
    await flusher.flush(record());
    expect(sent).toHaveLength(50);
  });

  it('bounds the sends in flight', async () => {
    const { flusher, maxInFlight } = harness({
      ids: Array.from({ length: 20 }, (_, i) => `c${i}`),
      concurrency: 4,
    });
    await flusher.flush(record());
    expect(maxInFlight()).toBeLessThanOrEqual(4);
  });

  it('fails the record when a delivery fails, after serving the healthy ones', async () => {
    const { flusher, sent } = harness({ ids: ['a', 'b', 'c'], failing: new Set(['b']) });
    await expect(flusher.flush(record())).rejects.toThrow(/1\/3 deliveries failed/);
    expect(sent.sort()).toEqual(['a', 'c']);
  });

  it('makes the retry idempotent: served once, the failed one re-served', async () => {
    const failing = new Set(['b']);
    const { flusher, sent } = harness({ ids: ['a', 'b', 'c'], failing });
    await flusher.flush(record()).catch(() => {});
    failing.clear();
    await flusher.flush(record()); // same messageId: the redelivery
    expect(sent.sort()).toEqual(['a', 'b', 'c']);
  });

  it('reaps a 410 instead of failing the record', async () => {
    const { flusher, sent, reaped } = harness({ ids: ['a', 'ghost'], gone: new Set(['ghost']) });
    await flusher.flush(record());
    expect(reaped).toEqual(['ghost']);
    expect(sent).toEqual(['a']);
  });

  it('walks every page when there is time', async () => {
    const { flusher, sent, requeued } = harness({
      ids: Array.from({ length: 25 }, (_, i) => `c${i}`),
      pageSize: 10,
    });
    await flusher.flush(record());
    expect(sent).toHaveLength(25);
    expect(requeued).toHaveLength(0);
  });

  it('requeues the remainder, same messageId plus a cursor, when time runs out', async () => {
    const { flusher, sent, requeued } = harness({
      ids: Array.from({ length: 25 }, (_, i) => `c${i}`),
      pageSize: 10,
    });
    await flusher.flush(record(), { remainingMs: () => 1 });
    expect(sent).toHaveLength(10);
    expect(requeued).toEqual([expect.objectContaining({ messageId: 'msg-1', cursor: { at: 10 } })]);
  });

  it('resumes a continuation from its cursor', async () => {
    const { flusher, sent } = harness({ ids: ['a', 'b', 'c', 'd'], pageSize: 2 });
    await flusher.flush(record({ cursor: { at: 2 } }));
    expect(sent).toEqual(['c', 'd']);
  });

  it('restores the record\'s endpoint before sending', async () => {
    const { flusher } = harness({ ids: [] });
    await flusher.flush(record({ endpoint: 'https://from-record/stage' }));
    expect(process.env.MANAGEMENT_ENDPOINT).toBe('https://from-record/stage');
  });

  it('refuses an unknown kind loudly, naming the ones it has', async () => {
    const { flusher } = harness({ ids: [] });
    await expect(flusher.flush(record({ kind: 'nope' }))).rejects.toThrow(
      /no fan-out resolver registered for kind "nope" \(have: test\)/,
    );
    const empty = new Flusher({
      ledger: new InMemoryDeliveryLedger(),
      bus: () => ({ publish: async () => {} }),
      onConnectionGone: async () => {},
    });
    await expect(empty.flush(record())).rejects.toThrow(/\(have: none\)/);
  });

  it('lets a later registration replace a kind', async () => {
    const { flusher } = harness({ ids: ['a'] });
    const replacement = vi.fn(async () => ({ targets: [] }));
    flusher.register('test', replacement);
    await flusher.flush(record());
    expect(replacement).toHaveBeenCalled();
    expect(flusher.kinds()).toEqual(['test']);
  });
});

describe('decodeStreamEvent', () => {
  const image = (pk: string, messageId: string, eventName = 'INSERT') => ({
    eventName,
    dynamodb: {
      SequenceNumber: `seq-${messageId}`,
      NewImage: {
        pk: { S: pk }, sk: { S: messageId }, expiresAt: { N: '1' },
        kind: { S: 'topic' }, topic: { S: pk }, messageId: { S: messageId },
        publishedAt: { N: '5' }, payload: { M: { n: { N: '1' } } },
      },
    },
  });

  it('groups INSERTs by partition key, in order, without the storage keys', () => {
    const groups = decodeStreamEvent({
      Records: [image('TOPIC#a', 'm1'), image('TOPIC#b', 'm2'), image('TOPIC#a', 'm3')],
    });
    expect([...groups.keys()]).toEqual(['TOPIC#a', 'TOPIC#b']);
    expect(groups.get('TOPIC#a')!.map(e => e.seq)).toEqual(['seq-m1', 'seq-m3']);
    expect(groups.get('TOPIC#a')![0].record).toEqual({
      kind: 'topic', topic: 'TOPIC#a', messageId: 'm1', publishedAt: 5, payload: { n: 1 },
    });
  });

  it('ignores everything that is not an INSERT with an image', () => {
    const groups = decodeStreamEvent({
      Records: [image('TOPIC#a', 'm1', 'REMOVE'), { eventName: 'INSERT', dynamodb: {} }],
    });
    expect(groups.size).toBe(0);
    expect(decodeStreamEvent({}).size).toBe(0);
  });
});
