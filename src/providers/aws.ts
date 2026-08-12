/* =============================================================================
 *  AWS-mode implementations — lazy @aws-sdk so local needs no creds
 * =============================================================================
 *  ConnectionStore over DynamoDB and a RealtimePublisher over the API Gateway
 *  @connections Management API. Instantiated directly (see runtime.ts), not via
 *  the Nest DI container. The SDK is require()d lazily so local never loads it.
 * ========================================================================== */

import {
  ConnectionStore,
  RealtimePublisher,
  SessionMeta,
  ConnectionGoneError,
  isConnectionGone,
  PageCursor,
} from '../ports';
import { docClient, queryAll, queryPage } from './dynamo';

export class DynamoConnectionStore implements ConnectionStore {
  private readonly table = process.env.CONNECTIONS_TABLE ?? 'connections';

  private client() {
    return docClient();
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
    // Drop the connection AND every room it was in. The reverse rows written by
    // join() (CONN#id / ROOM#room) let us find them with a single-partition
    // query — without them, membersOf() would keep returning this dead id
    // forever and every broadcast would waste a doomed send on it.
    //
    // queryAll, not one Query: a connection in more rooms than fit in 1MB would
    // otherwise keep the rooms this read never saw, forever.
    const owned = await queryAll(this.client(), {
      TableName: this.table,
      KeyConditionExpression: 'pk = :pk',
      ExpressionAttributeValues: { ':pk': `CONN#${id}` },
    });
    await Promise.all(
      owned.flatMap((item: any) => {
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
  private membersQuery(room: string) {
    return {
      TableName: this.table,
      KeyConditionExpression: 'pk = :pk',
      ExpressionAttributeValues: { ':pk': `ROOM#${room}` },
      // A membership row written moments ago (client.join() during $connect) must
      // be visible to the very next broadcast, and an eventually-consistent read
      // can miss it — which reads as "the first message never arrived".
      ConsistentRead: true,
    };
  }

  private static ids(items: any[]) {
    return items.map((i: any) => String(i.sk).replace('CONN#', ''));
  }

  async membersOf(room: string) {
    return DynamoConnectionStore.ids(await queryAll(this.client(), this.membersQuery(room)));
  }

  /** Paged form for the fan-out path — see ConnectionStore.pageMembersOf. */
  async pageMembersOf(room: string, cursor?: PageCursor) {
    const page = await queryPage(this.client(), this.membersQuery(room), cursor);
    return { items: DynamoConnectionStore.ids(page.items), cursor: page.cursor };
  }
}

export class ApiGatewayPublisher implements RealtimePublisher {
  private clients = new Map<string, any>();
  constructor(private readonly store: ConnectionStore) {}

  private mgmt(): any {
    // The @connections endpoint is per domain/stage. On the WebSocket path the
    // gateway dispatch derives it per-request; on the flush path it travels with
    // the outbox record (see Flusher.flush). Both write process.env.
    const endpoint = process.env.MANAGEMENT_ENDPOINT;
    if (!endpoint) {
      throw new Error(
        'MANAGEMENT_ENDPOINT is unset, so there is nowhere to post to. It is normally set from the ' +
          'API Gateway event, or carried on the outbox record; a flush Lambda invoked outside both ' +
          'needs it in the environment.',
      );
    }
    let c = this.clients.get(endpoint);
    if (!c) {
      const { ApiGatewayManagementApiClient } = require('@aws-sdk/client-apigatewaymanagementapi');
      c = new ApiGatewayManagementApiClient({ endpoint });
      this.clients.set(endpoint, c);
    }
    return c;
  }

  async toConnection(id: string, event: string, data: unknown) {
    await this.post(id, { event, data });
  }
  /** Verbatim payload — used by transports with their own wire format (GraphQL). */
  async toConnectionRaw(id: string, payload: unknown) {
    await this.post(id, payload);
  }

  private async post(id: string, payload: unknown) {
    const { PostToConnectionCommand, GoneException } = require('@aws-sdk/client-apigatewaymanagementapi');
    try {
      await this.mgmt().send(
        new PostToConnectionCommand({
          ConnectionId: id,
          Data: Buffer.from(JSON.stringify(payload)),
        }),
      );
    } catch (err: any) {
      if (err instanceof GoneException || err?.name === 'GoneException') {
        throw new ConnectionGoneError(id); // 410 — caller cleans up the ghost
      }
      throw err;
    }
  }

  /** IMMEDIATE, best-effort fan-out — a failed send here is logged and lost.
   *  This is NOT the path `server.to(room).emit()` takes: that publishes to the
   *  outbox so delivery is retried until it succeeds (see src/outbox.ts). Kept as
   *  a primitive for callers holding the publisher directly. */
  async toRoom(room: string, event: string, data: unknown) {
    const ids = await this.store.membersOf(room);
    await Promise.allSettled(
      ids.map(async (id) => {
        try {
          await this.toConnection(id, event, data);
        } catch (e) {
          if (isConnectionGone(e)) await this.store.remove(id);
          else throw e;
        }
      }),
    );
  }
}
