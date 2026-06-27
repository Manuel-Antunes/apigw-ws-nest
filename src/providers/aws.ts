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
    const { DeleteCommand } = require('@aws-sdk/lib-dynamodb');
    await this.client().send(
      new DeleteCommand({ TableName: this.table, Key: { pk: `CONN#${id}`, sk: 'META' } }),
    );
  }
  async join(id: string, room: string) {
    const { PutCommand } = require('@aws-sdk/lib-dynamodb');
    // Interest map row, queryable by room via the partition key.
    await this.client().send(
      new PutCommand({ TableName: this.table, Item: { pk: `ROOM#${room}`, sk: `CONN#${id}` } }),
    );
  }
  async leave(id: string, room: string) {
    const { DeleteCommand } = require('@aws-sdk/lib-dynamodb');
    await this.client().send(
      new DeleteCommand({ TableName: this.table, Key: { pk: `ROOM#${room}`, sk: `CONN#${id}` } }),
    );
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
