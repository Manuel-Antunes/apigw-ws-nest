/* =============================================================================
 *  GqlSubscriptionRegistry — durable "who is subscribed to what".
 * =============================================================================
 *  The registry is the whole reason GraphQL subscriptions survive a stateless
 *  runtime. A live AsyncIterator can't outlive a Lambda invocation, so instead we
 *  persist everything needed to RE-RUN the subscription later, from any instance:
 *  the connection, the client's subscription id, the topics it awaits, and the
 *  original document + variables.
 *
 *  On publish the transport reads the rows for that topic and replays each one.
 *
 *  DynamoDB layout — shares CONNECTIONS_TABLE with the connection registry, same
 *  composite pk/sk, so there's no second table to provision:
 *
 *    pk                     | sk                        | meaning
 *    -----------------------|---------------------------|------------------------
 *    GQLTOPIC#<topic>       | CONN#<conn>#SUB#<sub>     | forward: fan-out lookup
 *    CONN#<conn>            | GQLSUB#<sub>              | reverse: cleanup index
 *
 *  The reverse row exists for the same reason the room reverse row does: without
 *  it, a $disconnect could not find the forward rows, and every later publish
 *  would replay a subscription for a socket that is long gone.
 * ========================================================================== */

import { PROVIDER } from '../index';

/** Everything needed to re-execute one client subscription from scratch. */
export interface GqlSubscriptionRecord {
  connectionId: string;
  /** The `id` the client chose in its `subscribe` message; echoed on every `next`. */
  subscriptionId: string;
  /** Topics the subscription's field(s) registered with ApiGwPubSub. */
  topics: string[];
  query: string;
  variables?: Record<string, unknown> | null;
  operationName?: string | null;
}

export interface GqlSubscriptionRegistry {
  add(record: GqlSubscriptionRecord): Promise<void>;
  /** Every subscription awaiting `topic`, across every instance. */
  byTopic(topic: string): Promise<GqlSubscriptionRecord[]>;
  /** One client `complete`. */
  remove(connectionId: string, subscriptionId: string): Promise<void>;
  /** $disconnect / 410 Gone — drop every subscription this connection held. */
  removeAll(connectionId: string): Promise<void>;
}

/* ---- local mode ---------------------------------------------------------- */

export class InMemorySubscriptionRegistry implements GqlSubscriptionRegistry {
  /** connectionId -> subscriptionId -> record */
  private readonly byConnection = new Map<string, Map<string, GqlSubscriptionRecord>>();

  async add(record: GqlSubscriptionRecord) {
    const subs =
      this.byConnection.get(record.connectionId) ??
      this.byConnection.set(record.connectionId, new Map()).get(record.connectionId)!;
    subs.set(record.subscriptionId, record);
  }
  async byTopic(topic: string) {
    const out: GqlSubscriptionRecord[] = [];
    for (const subs of this.byConnection.values()) {
      for (const record of subs.values()) {
        if (record.topics.includes(topic)) out.push(record);
      }
    }
    return out;
  }
  async remove(connectionId: string, subscriptionId: string) {
    this.byConnection.get(connectionId)?.delete(subscriptionId);
  }
  async removeAll(connectionId: string) {
    this.byConnection.delete(connectionId);
  }
}

/* ---- aws mode ------------------------------------------------------------ */

const topicPk = (topic: string) => `GQLTOPIC#${topic}`;
const connPk = (connectionId: string) => `CONN#${connectionId}`;
const forwardSk = (connectionId: string, subscriptionId: string) =>
  `CONN#${connectionId}#SUB#${subscriptionId}`;
const reverseSk = (subscriptionId: string) => `GQLSUB#${subscriptionId}`;

export class DynamoSubscriptionRegistry implements GqlSubscriptionRegistry {
  private doc: any;
  private readonly table = process.env.CONNECTIONS_TABLE ?? 'connections';

  private client() {
    if (!this.doc) {
      // eslint-disable-next-line @typescript-eslint/no-var-requires
      const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
      // eslint-disable-next-line @typescript-eslint/no-var-requires
      const { DynamoDBDocumentClient } = require('@aws-sdk/lib-dynamodb');
      // GraphQL variables routinely contain undefined; drop them rather than
      // failing the whole subscribe.
      this.doc = DynamoDBDocumentClient.from(new DynamoDBClient({}), {
        marshallOptions: { removeUndefinedValues: true },
      });
    }
    return this.doc;
  }

  async add(record: GqlSubscriptionRecord) {
    const { PutCommand } = require('@aws-sdk/lib-dynamodb');
    const sk = forwardSk(record.connectionId, record.subscriptionId);
    await Promise.all([
      // One forward row per topic — a publish is then a single-partition query.
      ...record.topics.map((topic: string) =>
        this.client().send(
          new PutCommand({
            TableName: this.table,
            Item: { pk: topicPk(topic), sk, ...record },
          }),
        ),
      ),
      // Reverse row so removeAll() can find those forward rows again.
      this.client().send(
        new PutCommand({
          TableName: this.table,
          Item: {
            pk: connPk(record.connectionId),
            sk: reverseSk(record.subscriptionId),
            topics: record.topics,
          },
        }),
      ),
    ]);
  }

  async byTopic(topic: string) {
    const { QueryCommand } = require('@aws-sdk/lib-dynamodb');
    const out = await this.client().send(
      new QueryCommand({
        TableName: this.table,
        KeyConditionExpression: 'pk = :pk',
        ExpressionAttributeValues: { ':pk': topicPk(topic) },
        // A DynamoDB Query is eventually consistent by default, and that is not
        // survivable here: a client that subscribes and then immediately triggers
        // a mutation can have its own row missing from this read, so the payload
        // is dropped — permanently, since a publish is never retried. Subscribe
        // then publish is the FIRST thing anyone does, so this shows up as
        // "the first event never arrives" and nothing else.
        ConsistentRead: true,
      }),
    );
    return (out.Items ?? []).map((item: any) => ({
      connectionId: String(item.connectionId),
      subscriptionId: String(item.subscriptionId),
      topics: (item.topics ?? []) as string[],
      query: String(item.query),
      variables: item.variables ?? null,
      operationName: item.operationName ?? null,
    })) as GqlSubscriptionRecord[];
  }

  async remove(connectionId: string, subscriptionId: string) {
    const { GetCommand } = require('@aws-sdk/lib-dynamodb');
    const reverse = await this.client().send(
      new GetCommand({
        TableName: this.table,
        Key: { pk: connPk(connectionId), sk: reverseSk(subscriptionId) },
        ConsistentRead: true,
      }),
    );
    await this.deleteRows(connectionId, subscriptionId, (reverse.Item?.topics ?? []) as string[]);
  }

  async removeAll(connectionId: string) {
    const { QueryCommand } = require('@aws-sdk/lib-dynamodb');
    const owned = await this.client().send(
      new QueryCommand({
        TableName: this.table,
        KeyConditionExpression: 'pk = :pk AND begins_with(sk, :prefix)',
        ExpressionAttributeValues: { ':pk': connPk(connectionId), ':prefix': 'GQLSUB#' },
        // Same reasoning as byTopic: a stale read here leaves forward rows behind,
        // and every later publish then replays a subscription for a dead socket.
        ConsistentRead: true,
      }),
    );
    await Promise.all(
      (owned.Items ?? []).map((item: any) =>
        this.deleteRows(
          connectionId,
          String(item.sk).slice('GQLSUB#'.length),
          (item.topics ?? []) as string[],
        ),
      ),
    );
  }

  /** Drop one subscription's forward rows (one per topic) plus its reverse row. */
  private async deleteRows(connectionId: string, subscriptionId: string, topics: string[]) {
    const { DeleteCommand } = require('@aws-sdk/lib-dynamodb');
    const sk = forwardSk(connectionId, subscriptionId);
    await Promise.all([
      ...topics.map((topic: string) =>
        this.client().send(
          new DeleteCommand({ TableName: this.table, Key: { pk: topicPk(topic), sk } }),
        ),
      ),
      this.client().send(
        new DeleteCommand({
          TableName: this.table,
          Key: { pk: connPk(connectionId), sk: reverseSk(subscriptionId) },
        }),
      ),
    ]);
  }
}

let _registry: GqlSubscriptionRegistry | undefined;

/**
 * Provider-appropriate registry — a PROCESS-level singleton, exactly like
 * runtime.ts's connectionStore()/publisher().
 *
 * The memoization is not an optimization, it's the contract: this registry
 * stands in for durable storage. In aws mode that's DynamoDB, which trivially
 * outlives any one Nest app. In local mode it's a Map, so it must NOT be rebuilt
 * when the app is — otherwise the local emulator's /__reload (which simulates a
 * redeploy by throwing the Nest app away) would wipe every subscription and
 * quietly stop reproducing the one failure mode this whole design exists to
 * prevent.
 */
export function subscriptionRegistry(): GqlSubscriptionRegistry {
  return (_registry ??=
    PROVIDER === 'aws' ? new DynamoSubscriptionRegistry() : new InMemorySubscriptionRegistry());
}
