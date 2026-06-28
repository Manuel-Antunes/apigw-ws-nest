/* =============================================================================
 *  AWS-mode implementations — lazy @aws-sdk so local needs no creds
 * =============================================================================
 *  ConnectionStore over DynamoDB and a RealtimePublisher over the API Gateway
 *  @connections Management API. Instantiated directly (see runtime.ts), not via
 *  the Nest DI container. The SDK is require()d lazily so local never loads it.
 * ========================================================================== */

import { ConnectionStore, RealtimePublisher, SessionMeta, ConnectionGoneError } from '../ports';

export class DynamoConnectionStore implements ConnectionStore {
  private doc: any;
  private readonly table = process.env.CONNECTIONS_TABLE ?? 'connections';

  private client() {
    if (!this.doc) {
      // eslint-disable-next-line @typescript-eslint/no-var-requires
      const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
      // eslint-disable-next-line @typescript-eslint/no-var-requires
      const { DynamoDBDocumentClient } = require('@aws-sdk/lib-dynamodb');
      this.doc = DynamoDBDocumentClient.from(new DynamoDBClient({}));
    }
    return this.doc;
  }

  async add(id: string, meta: SessionMeta) {
    const { PutCommand } = require('@aws-sdk/lib-dynamodb');
    // ttl guards against $disconnect never firing (best-effort event).
    const ttl = Math.floor(Date.now() / 1000) + 60 * 60 * 3;
    await this.client().send(
      new PutCommand({ TableName: this.table, Item: { pk: `CONN#${id}`, sk: 'META', ...meta, ttl } }),
    );
  }
  async remove(id: string) {
    const { QueryCommand, DeleteCommand } = require('@aws-sdk/lib-dynamodb');
    // Drop the connection AND every room it was in. The reverse rows written by
    // join() (CONN#id / ROOM#room) let us find them with a single-partition
    // query — without them, membersOf() would keep returning this dead id
    // forever and every broadcast would waste a doomed send on it.
    const owned = await this.client().send(
      new QueryCommand({
        TableName: this.table,
        KeyConditionExpression: 'pk = :pk',
        ExpressionAttributeValues: { ':pk': `CONN#${id}` },
      }),
    );
    await Promise.all(
      (owned.Items ?? []).flatMap((item: any) => {
        const sk = String(item.sk);
        // Always delete the owned row (META + each ROOM# reverse row).
        const deletes = [
          this.client().send(
            new DeleteCommand({ TableName: this.table, Key: { pk: `CONN#${id}`, sk } }),
          ),
        ];
        // For a reverse room row, also delete the mirrored forward membership row.
        if (sk.startsWith('ROOM#')) {
          const room = sk.slice('ROOM#'.length);
          deletes.push(
            this.client().send(
              new DeleteCommand({ TableName: this.table, Key: { pk: `ROOM#${room}`, sk: `CONN#${id}` } }),
            ),
          );
        }
        return deletes;
      }),
    );
  }
  async join(id: string, room: string) {
    const { PutCommand } = require('@aws-sdk/lib-dynamodb');
    // Two mirrored rows: a FORWARD row queryable by room (membersOf) and a
    // REVERSE row queryable by connection (so remove() can clear every room on
    // disconnect — API Gateway's $disconnect is best-effort, but so is the ttl).
    await Promise.all([
      this.client().send(
        new PutCommand({ TableName: this.table, Item: { pk: `ROOM#${room}`, sk: `CONN#${id}` } }),
      ),
      this.client().send(
        new PutCommand({ TableName: this.table, Item: { pk: `CONN#${id}`, sk: `ROOM#${room}` } }),
      ),
    ]);
  }
  async leave(id: string, room: string) {
    const { DeleteCommand } = require('@aws-sdk/lib-dynamodb');
    await Promise.all([
      this.client().send(
        new DeleteCommand({ TableName: this.table, Key: { pk: `ROOM#${room}`, sk: `CONN#${id}` } }),
      ),
      this.client().send(
        new DeleteCommand({ TableName: this.table, Key: { pk: `CONN#${id}`, sk: `ROOM#${room}` } }),
      ),
    ]);
  }
  async membersOf(room: string) {
    const { QueryCommand } = require('@aws-sdk/lib-dynamodb');
    const out = await this.client().send(
      new QueryCommand({
        TableName: this.table,
        KeyConditionExpression: 'pk = :pk',
        ExpressionAttributeValues: { ':pk': `ROOM#${room}` },
      }),
    );
    return (out.Items ?? []).map((i: any) => String(i.sk).replace('CONN#', ''));
  }
}

export class ApiGatewayPublisher implements RealtimePublisher {
  private clients = new Map<string, any>();
  constructor(private readonly store: ConnectionStore) {}

  private mgmt(): any {
    // The @connections endpoint is per domain/stage; the gateway dispatch derives
    // it per-request (process.env.MANAGEMENT_ENDPOINT).
    const endpoint = process.env.MANAGEMENT_ENDPOINT!;
    let c = this.clients.get(endpoint);
    if (!c) {
      const { ApiGatewayManagementApiClient } = require('@aws-sdk/client-apigatewaymanagementapi');
      c = new ApiGatewayManagementApiClient({ endpoint });
      this.clients.set(endpoint, c);
    }
    return c;
  }

  async toConnection(id: string, event: string, data: unknown) {
    const { PostToConnectionCommand, GoneException } = require('@aws-sdk/client-apigatewaymanagementapi');
    try {
      await this.mgmt().send(
        new PostToConnectionCommand({
          ConnectionId: id,
          Data: Buffer.from(JSON.stringify({ event, data })),
        }),
      );
    } catch (err: any) {
      if (err instanceof GoneException || err?.name === 'GoneException') {
        throw new ConnectionGoneError(id); // 410 — caller cleans up the ghost
      }
      throw err;
    }
  }

  async toRoom(room: string, event: string, data: unknown) {
    const ids = await this.store.membersOf(room);
    // NOTE: large fan-out = N HTTP calls. Past a threshold, re-publish to SQS
    // and let workers parallelize instead of looping here. Kept simple.
    await Promise.allSettled(
      ids.map(async (id) => {
        try {
          await this.toConnection(id, event, data);
        } catch (e) {
          if (e instanceof ConnectionGoneError) await this.store.remove(id);
          else throw e;
        }
      }),
    );
  }
}
