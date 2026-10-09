/* =============================================================================
 *  GqlSubscriptionRegistry — durable "who is subscribed to what".
 * =============================================================================
 *  The registry is the whole reason GraphQL subscriptions survive a stateless
 *  runtime. A live AsyncIterator can't outlive a Lambda invocation, so instead we
 *  persist everything needed to RE-RUN the subscription later, from any instance:
 *  the connection, the client's subscription id, the topics it awaits, the
 *  original document + variables, and the connection's identity (client.data).
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

import { Page, PageCursor } from '../index';
import { docClient, queryAll, queryPage } from '../providers/dynamo';

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
  /** The connection's `client.data` when it subscribed — what a replay puts in
   *  `context.connection.data` without reading the connection back. Exact,
   *  because data cannot change after $connect. Absent when it was empty. */
  connectionData?: Record<string, unknown>;
}

export interface GqlSubscriptionRegistry {
  add(record: GqlSubscriptionRecord): Promise<void>;
  /** Every subscription awaiting `topic`, across every instance. */
  byTopic(topic: string): Promise<GqlSubscriptionRecord[]>;
  /** Paged form, used by the flush path so a topic with more subscribers than
   *  fit in one page is delivered across several invocations instead of being
   *  loaded whole. Optional: a custom registry without it falls back to
   *  byTopic(). */
  pageByTopic?(topic: string, cursor?: PageCursor): Promise<Page<GqlSubscriptionRecord>>;
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
  /** In-memory: always one page. Present so the flush path takes the same branch
   *  locally as it does on DynamoDB. */
  async pageByTopic(topic: string) {
    return { items: await this.byTopic(topic), cursor: undefined };
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
  private readonly table = process.env.CONNECTIONS_TABLE ?? 'connections';

  private client() {
    // GraphQL variables routinely contain undefined; drop them rather than
    // failing the whole subscribe.
    return docClient('gql', { marshallOptions: { removeUndefinedValues: true } });
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

  private topicQuery(topic: string) {
    return {
      TableName: this.table,
      KeyConditionExpression: 'pk = :pk',
      ExpressionAttributeValues: { ':pk': topicPk(topic) },
      // A DynamoDB Query is eventually consistent by default, and that is not
      // survivable here: a client that subscribes and then immediately triggers
      // a mutation can have its own row missing from this read, so the payload
      // is dropped. Subscribe then publish is the FIRST thing anyone does, so
      // this shows up as "the first event never arrives" and nothing else.
      ConsistentRead: true,
    };
  }

  private static hydrate(items: any[]): GqlSubscriptionRecord[] {
    return items.map((item: any) => ({
      connectionId: String(item.connectionId),
      subscriptionId: String(item.subscriptionId),
      topics: (item.topics ?? []) as string[],
      query: String(item.query),
      variables: item.variables ?? null,
      operationName: item.operationName ?? null,
      ...(item.connectionData ? { connectionData: item.connectionData } : {}),
    }));
  }

  /** Every subscriber. Pages internally — a single Query stops at 1MB and returns
   *  a PREFIX with no error, which here would mean the subscribers past that
   *  point silently stop receiving anything. */
  async byTopic(topic: string) {
    return DynamoSubscriptionRegistry.hydrate(
      await queryAll(this.client(), this.topicQuery(topic)),
    );
  }

  async pageByTopic(topic: string, cursor?: PageCursor) {
    const page = await queryPage(this.client(), this.topicQuery(topic), cursor);
    return { items: DynamoSubscriptionRegistry.hydrate(page.items), cursor: page.cursor };
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
    // queryAll, not one Query: a leftover forward row is replayed by every later
    // publish to that topic, forever, against a socket that no longer exists.
    const owned = await queryAll(this.client(), {
      TableName: this.table,
      KeyConditionExpression: 'pk = :pk AND begins_with(sk, :prefix)',
      ExpressionAttributeValues: { ':pk': connPk(connectionId), ':prefix': 'GQLSUB#' },
      // Same reasoning as byTopic: a stale read here leaves forward rows behind.
      ConsistentRead: true,
    });
    await Promise.all(
      owned.map((item: any) =>
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

/* There is deliberately no subscriptionRegistry() singleton here any more.
 *
 * GraphQLWsHandler defaults the registry from the bridge's provider (DynamoDB in
 * aws mode, in-memory in local). If you rebuild the bridge while sockets stay
 * open — which the local emulator does on every simulated redeploy — construct
 * ONE InMemorySubscriptionRegistry yourself and pass it in each time, so the
 * subscriptions outlive the rebuild. That used to be a hidden memoization; it is
 * now a visible variable in the emulator, where it belongs, because "this thing
 * must survive the app" is exactly what the local emulator exists to test. */
