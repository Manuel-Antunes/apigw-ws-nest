/* =============================================================================
 *  Outbox — durable fan-out. Publish is a write; delivery is a stream consumer.
 * =============================================================================
 *  THE PROBLEM. Fan-out used to happen inline, inside whichever invocation called
 *  server.to(room).emit() or pubsub.publish(): look the subscribers up, then post
 *  to each one, all before the handler could return. Two consequences, both bad:
 *
 *    - A failed delivery was logged and DROPPED. Nothing retried it, because
 *      nothing had recorded that it was owed. A Lambda timeout mid-fan-out lost
 *      every remaining subscriber silently.
 *    - The publisher paid for every subscriber. One mutation, 500 sockets, 500
 *      sequential HTTP calls on the write path.
 *
 *  THE SHAPE. Publishing writes ONE row to the Messages table and returns. That
 *  table has a DynamoDB Stream, so the write itself is the trigger:
 *
 *      publish()  ->  PutItem (Messages)  ->  Stream  ->  bridge.flush()
 *                                                            |
 *                                    page subscribers  <-----+
 *                                    claim + deliver each
 *
 *  What this buys, in order of why it matters here:
 *
 *  1. DELIVERY IS RETRIED. A stream record is redelivered until the consumer
 *     reports success or the record ages out (24h), with a DLQ underneath.
 *  2. ONE INVOCATION PER TOPIC, NOT PER SUBSCRIBER. A broadcast is a single row,
 *     so one flush walks that topic's whole subscriber list and batches the sends.
 *  3. ORDER PER TOPIC. The partition key is the topic/room, and a stream
 *     preserves order within a partition key.
 *
 *  THE PART STREAMS DO NOT GIVE YOU. Stream delivery is AT-LEAST-ONCE: a batch
 *  that fails partway is redelivered whole. Streams create the need for
 *  idempotency rather than providing it — hence the DeliveryLedger, which claims
 *  each (message, subscriber) pair before sending and releases the claim if the
 *  send fails.
 *
 *  Nothing in this file is a singleton. A Flusher is owned by the GatewayBridge
 *  that built it, and its resolvers come from the ProtocolHandlers registered on
 *  that bridge.
 * ========================================================================== */

import { randomUUID } from 'crypto';
import {
  Broadcast,
  DeliveryLedger,
  DeliveryTarget,
  FanoutResolver,
  MessageBus,
  OutboxRecord,
  PageCursor,
  isConnectionGone,
} from './ports';
import { docClient } from './providers/dynamo';

/** The partition key. Topic/room granularity is deliberate: it is simultaneously
 *  the fan-out unit, the stream ordering unit, and the DynamoDB partition. */
export const outboxPk = (message: Broadcast): string =>
  message.kind === 'room'
    ? `ROOM#${(message as any).room}`
    : `TOPIC#${(message as any).topic ?? message.kind}`;

/** Undelivered messages are garbage after this; the table's TTL reaps them. */
export const OUTBOX_TTL_SECONDS = 60 * 60;

/** Stamp identity onto a broadcast, unless it already carries one (a retry or a
 *  continuation must keep the original messageId — it is the dedupe key). */
export function sealRecord(message: Broadcast | OutboxRecord): OutboxRecord {
  if ('messageId' in message && message.messageId) return message as OutboxRecord;
  return {
    ...(message as Broadcast),
    messageId: randomUUID(),
    publishedAt: Date.now(),
    ...(process.env.MANAGEMENT_ENDPOINT ? { endpoint: process.env.MANAGEMENT_ENDPOINT } : {}),
  } as OutboxRecord;
}

/* ---- buses --------------------------------------------------------------- */

/** aws mode. One PutItem, O(1) regardless of subscriber count — the stream does
 *  the rest. */
export class DynamoOutboxBus implements MessageBus {
  constructor(private readonly table = process.env.MESSAGES_TABLE ?? 'messages') {}

  async publish(message: Broadcast | OutboxRecord) {
    const record = sealRecord(message);
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const { PutCommand } = require('@aws-sdk/lib-dynamodb');
    await docClient('outbox', { marshallOptions: { removeUndefinedValues: true } }).send(
      new PutCommand({
        TableName: this.table,
        Item: {
          pk: outboxPk(record),
          // Time-ordered and unique. A continuation reuses messageId but takes a
          // fresh sk, so it is a genuinely new stream record.
          sk: `${String(record.publishedAt).padStart(13, '0')}#${randomUUID()}`,
          ...record,
          expiresAt: Math.floor(Date.now() / 1000) + OUTBOX_TTL_SECONDS,
        },
      }),
    );
  }
}

/** local mode. No table, no stream — deliver right here, awaited. Same Flusher,
 *  same resolvers, same ledger, so the emulator exercises the production path
 *  rather than a simplified stand-in; only the trigger differs. */
export class InlineMessageBus implements MessageBus {
  constructor(private readonly deliver: (record: OutboxRecord) => Promise<void>) {}
  async publish(message: Broadcast | OutboxRecord) {
    await this.deliver(sealRecord(message));
  }
}

/* ---- ledgers ------------------------------------------------------------- */

export class InMemoryDeliveryLedger implements DeliveryLedger {
  private readonly seen = new Map<string, Set<string>>();
  async claim(messageId: string, key: string) {
    const keys = this.seen.get(messageId) ?? this.seen.set(messageId, new Set()).get(messageId)!;
    if (keys.has(key)) return false;
    keys.add(key);
    return true;
  }
  async release(messageId: string, key: string) {
    this.seen.get(messageId)?.delete(key);
  }
}

export class DynamoDeliveryLedger implements DeliveryLedger {
  constructor(private readonly table = process.env.CONNECTIONS_TABLE ?? 'connections') {}

  /** One partition PER DELIVERY, not per message: a 10k-subscriber broadcast
   *  would otherwise slam 10k writes into a single partition and throttle. */
  private key(messageId: string, key: string) {
    return { pk: `DLV#${messageId}#${key}`, sk: 'DLV' };
  }

  async claim(messageId: string, key: string) {
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const { PutCommand } = require('@aws-sdk/lib-dynamodb');
    try {
      await docClient().send(
        new PutCommand({
          TableName: this.table,
          Item: {
            ...this.key(messageId, key),
            ttl: Math.floor(Date.now() / 1000) + OUTBOX_TTL_SECONDS,
          },
          ConditionExpression: 'attribute_not_exists(pk)',
        }),
      );
      return true;
    } catch (err: any) {
      if (err?.name === 'ConditionalCheckFailedException') return false;
      throw err;
    }
  }

  async release(messageId: string, key: string) {
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const { DeleteCommand } = require('@aws-sdk/lib-dynamodb');
    await docClient().send(
      new DeleteCommand({ TableName: this.table, Key: this.key(messageId, key) }),
    );
  }
}

/* ---- the flusher --------------------------------------------------------- */

export interface FlushContext {
  /** Lambda's getRemainingTimeInMillis. Drives the continuation decision. */
  remainingMs?: () => number;
}

export interface FlusherOptions {
  ledger: DeliveryLedger;
  /** Requeue the remainder rather than starting another page. */
  bus: () => MessageBus;
  /** A 410 Gone during delivery — reap the socket. */
  onConnectionGone: (connectionId: string) => Promise<void>;
  /** Sends in flight at once. Bounded so a huge topic can't open ten thousand
   *  connections to the @connections API simultaneously. */
  concurrency?: number;
  /** Don't start another page with less than this much time left. Also protects
   *  the ledger: a claim whose send is killed by a timeout is the one window
   *  where a message can be lost, and this keeps invocations from dying
   *  mid-delivery. */
  reserveMs?: number;
}

export class Flusher {
  private readonly resolvers = new Map<string, FanoutResolver>();
  private readonly concurrency: number;
  private readonly reserveMs: number;

  constructor(private readonly options: FlusherOptions) {
    this.concurrency = options.concurrency ?? 25;
    this.reserveMs = options.reserveMs ?? 15_000;
  }

  /** Teach the flusher a kind. Last registration wins. */
  register(kind: string, resolver: FanoutResolver) {
    this.resolvers.set(kind, resolver);
  }

  kinds(): string[] {
    return [...this.resolvers.keys()];
  }

  /**
   * Deliver one record to every subscriber, paging until done.
   *
   * Throws if any delivery failed — which is the point: on the stream path that
   * marks the record unprocessed so Lambda redelivers it, and the ledger makes
   * the re-run skip whoever already got it.
   */
  async flush(record: OutboxRecord, ctx: FlushContext = {}): Promise<void> {
    const resolve = this.resolvers.get(record.kind);
    if (!resolve) {
      throw new Error(
        `outbox: no fan-out resolver registered for kind "${record.kind}" (have: ${
          this.kinds().join(', ') || 'none'
        }). A ProtocolHandler contributes kinds through its \`fanout\` map; register it with bridge.use().`,
      );
    }
    // A stream event has no requestContext, so the endpoint travels with the
    // record. Restore it before anything tries to post.
    if (record.endpoint) process.env.MANAGEMENT_ENDPOINT = record.endpoint;

    let cursor = record.cursor;
    for (;;) {
      const page = await resolve(record, cursor);
      await this.deliverPage(record, page.targets);
      cursor = page.cursor;
      if (!cursor) return;
      const left = ctx.remainingMs?.() ?? Infinity;
      if (left < this.reserveMs) {
        // Out of runway. Hand the rest to a fresh invocation instead of being
        // killed halfway through it — same messageId, so the ledger still
        // suppresses anything already delivered.
        await this.options.bus().publish({ ...record, cursor } as OutboxRecord);
        return;
      }
    }
  }

  private async deliverPage(record: OutboxRecord, targets: DeliveryTarget[]) {
    const failures: unknown[] = [];
    for (let i = 0; i < targets.length; i += this.concurrency) {
      const batch = targets.slice(i, i + this.concurrency);
      const settled = await Promise.allSettled(batch.map(t => this.deliverOne(record, t)));
      for (const outcome of settled) {
        if (outcome.status === 'rejected') failures.push(outcome.reason);
      }
    }
    if (failures.length) {
      // eslint-disable-next-line no-console
      failures.forEach(reason => console.error('[flush delivery]', reason));
      throw new Error(
        `outbox: ${failures.length}/${targets.length} deliveries failed for ${record.kind} message ${record.messageId}; the record will be retried`,
      );
    }
  }

  private async deliverOne(record: OutboxRecord, target: DeliveryTarget) {
    if (!(await this.options.ledger.claim(record.messageId, target.key))) return; // served
    try {
      await target.send();
    } catch (err) {
      if (isConnectionGone(err)) {
        // Not a failure: the socket is a ghost. Reap it and KEEP the claim, so a
        // retry doesn't waste another doomed send on it.
        await this.options.onConnectionGone(target.connectionId);
        return;
      }
      // Give the claim back before propagating, or the retry would skip this
      // subscriber and the message would be lost for them specifically.
      await this.options.ledger.release(record.messageId, target.key);
      throw err;
    }
  }
}

/* ---- stream event shapes ------------------------------------------------- */

/** Minimal shape of a DynamoDB Stream event (avoids a hard @types/aws-lambda dep). */
export interface StreamEvent {
  Records?: Array<{
    eventName?: string;
    dynamodb?: { NewImage?: Record<string, any>; SequenceNumber?: string };
  }>;
}

export interface StreamContext {
  getRemainingTimeInMillis?: () => number;
}

export interface BatchResponse {
  batchItemFailures: Array<{ itemIdentifier: string }>;
}

/** One stream record, unwrapped into (partition key, sequence, record). Exported
 *  because bridge.flush() is the only caller and this keeps it readable. */
export function decodeStreamEvent(
  event: StreamEvent,
): Map<string, Array<{ seq: string; record: OutboxRecord }>> {
  // eslint-disable-next-line @typescript-eslint/no-var-requires
  const { unmarshall } = require('@aws-sdk/util-dynamodb');
  const groups = new Map<string, Array<{ seq: string; record: OutboxRecord }>>();
  for (const raw of event.Records ?? []) {
    if (raw.eventName !== 'INSERT' || !raw.dynamodb?.NewImage) continue;
    const item = unmarshall(raw.dynamodb.NewImage as any) as any;
    const { pk, sk, expiresAt, ...record } = item;
    const group = groups.get(pk) ?? groups.set(pk, []).get(pk)!;
    group.push({ seq: String(raw.dynamodb.SequenceNumber), record: record as OutboxRecord });
  }
  return groups;
}

export type { PageCursor };
