import { createRequire } from 'node:module';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  ApiGatewayPublisher,
  ConnectionGoneError,
  DynamoConnectionStore,
  InMemoryConnectionStore,
} from '../../../src';
import { fakeDynamo } from '../../helpers/fake-dynamo';

vi.mock('../../../src/providers/dynamo', async importOriginal => {
  const { fakeDynamo } = await import('../../helpers/fake-dynamo');
  return {
    ...(await importOriginal<typeof import('../../../src/providers/dynamo')>()),
    docClient: (_key?: string, options?: any) => fakeDynamo.client(options),
  };
});

// The publisher require()s the SDK lazily; require it the same way so the spy
// lands on the very class it instantiates.
const sdk = createRequire(import.meta.url)('@aws-sdk/client-apigatewaymanagementapi');

const TABLE = 'connections';

beforeEach(() => fakeDynamo.reset());
afterEach(() => {
  vi.unstubAllEnvs();
  vi.useRealTimers();
  delete process.env.MANAGEMENT_ENDPOINT;
});

describe('DynamoConnectionStore', () => {
  it('writes META with a ttl past API Gateway\'s two-hour limit', async () => {
    vi.useFakeTimers({ now: 1_000_000_000_000, toFake: ['Date'] });
    await new DynamoConnectionStore().add('a', { connectedAt: 5, subprotocol: 'graphql-transport-ws' });
    expect(fakeDynamo.items(TABLE)).toEqual([
      { pk: 'CONN#a', sk: 'META', connectedAt: 5, subprotocol: 'graphql-transport-ws', ttl: 1_000_000_000 + 3 * 3600 },
    ]);
  });

  it('leaves absent optional fields out — this client does not remove undefined', async () => {
    await new DynamoConnectionStore().add('a', {
      connectedAt: 1, subprotocol: undefined, sourceIp: undefined, data: { user: 'ada' },
    });
    expect(fakeDynamo.items(TABLE)[0]).toEqual({
      pk: 'CONN#a', sk: 'META', connectedAt: 1, data: { user: 'ada' }, ttl: expect.any(Number),
    });
  });

  it('reads META back, strongly consistent, without the storage keys', async () => {
    const store = new DynamoConnectionStore();
    await store.add('a', { connectedAt: 1, sourceIp: '1.2.3.4', data: { user: 'ada' } });
    expect(await store.get('a')).toEqual({ connectedAt: 1, sourceIp: '1.2.3.4', data: { user: 'ada' } });
    expect(fakeDynamo.calls.find(c => c.command === 'GetCommand')!.input.ConsistentRead).toBe(true);
  });

  it('answers null for an unknown connection, and for an expired one DynamoDB has not reaped yet', async () => {
    const store = new DynamoConnectionStore();
    expect(await store.get('nobody')).toBeNull();
    fakeDynamo.seed(TABLE, { pk: 'CONN#old', sk: 'META', connectedAt: 1, ttl: Math.floor(Date.now() / 1000) - 1 });
    expect(await store.get('old')).toBeNull();
    fakeDynamo.seed(TABLE, { pk: 'CONN#new', sk: 'META', connectedAt: 1, ttl: Math.floor(Date.now() / 1000) + 60 });
    expect(await store.get('new')).toEqual({ connectedAt: 1 });
  });

  it('mirrors a membership into a forward and a reverse row', async () => {
    const store = new DynamoConnectionStore();
    await store.join('a', 'lobby');
    expect(fakeDynamo.items(TABLE)).toEqual([
      { pk: 'CONN#a', sk: 'ROOM#lobby' },
      { pk: 'ROOM#lobby', sk: 'CONN#a' },
    ]);
    await store.leave('a', 'lobby');
    expect(fakeDynamo.items(TABLE)).toEqual([]);
  });

  it('lists members with a consistent read, across pages', async () => {
    const store = new DynamoConnectionStore();
    for (const id of ['a', 'b', 'c']) await store.join(id, 'r');
    fakeDynamo.pageSize = 2;
    expect(await store.membersOf('r')).toEqual(['a', 'b', 'c']);
    const query = fakeDynamo.calls.find(c => c.command === 'QueryCommand')!;
    expect(query.input.ConsistentRead).toBe(true);

    const first = await store.pageMembersOf('r');
    expect(first.items).toEqual(['a', 'b']);
    expect(first.cursor).toBeDefined();
    const second = await store.pageMembersOf('r', first.cursor);
    expect(second).toEqual({ items: ['c'], cursor: undefined });
  });

  it('removes META, every reverse room row and its forward twin — and nothing else', async () => {
    const store = new DynamoConnectionStore();
    await store.add('a', { connectedAt: 1 });
    await store.join('a', 'r1');
    await store.join('a', 'r2');
    await store.join('b', 'r1');
    await store.remove('a');
    expect(fakeDynamo.items(TABLE)).toEqual([
      { pk: 'CONN#b', sk: 'ROOM#r1' },
      { pk: 'ROOM#r1', sk: 'CONN#b' },
    ]);
  });

  it('takes its table from CONNECTIONS_TABLE', async () => {
    vi.stubEnv('CONNECTIONS_TABLE', 'custom');
    await new DynamoConnectionStore().add('a', { connectedAt: 1 });
    expect(fakeDynamo.items('custom')).toHaveLength(1);
  });
});

describe('ApiGatewayPublisher', () => {
  const ENDPOINT = 'https://abc.execute-api.us-east-1.amazonaws.com/prod';
  const gone = () => new sdk.GoneException({ message: 'gone', $metadata: {} });

  function stubSend(impl: (command: any) => unknown = async () => ({})) {
    process.env.MANAGEMENT_ENDPOINT = ENDPOINT;
    return vi.spyOn(sdk.ApiGatewayManagementApiClient.prototype, 'send').mockImplementation(impl as any);
  }

  it('needs an endpoint to post to', async () => {
    await expect(new ApiGatewayPublisher(new InMemoryConnectionStore()).toConnection('a', 'e', 1)).rejects.toThrow(
      /MANAGEMENT_ENDPOINT is unset/,
    );
  });

  it('posts { event, data } frames, and raw payloads verbatim', async () => {
    const send = stubSend();
    const publisher = new ApiGatewayPublisher(new InMemoryConnectionStore());
    await publisher.toConnection('a', 'hello', { n: 1 });
    await publisher.toConnectionRaw('a', { type: 'next' });
    const [first, second] = send.mock.calls.map(([command]: any) => command);
    expect(first.constructor.name).toBe('PostToConnectionCommand');
    expect(first.input.ConnectionId).toBe('a');
    expect(JSON.parse(Buffer.from(first.input.Data).toString())).toEqual({ event: 'hello', data: { n: 1 } });
    expect(JSON.parse(Buffer.from(second.input.Data).toString())).toEqual({ type: 'next' });
  });

  it('builds one client per endpoint', async () => {
    stubSend();
    const publisher = new ApiGatewayPublisher(new InMemoryConnectionStore());
    await publisher.toConnection('a', 'e', 1);
    await publisher.toConnection('b', 'e', 1);
    process.env.MANAGEMENT_ENDPOINT = 'https://other/stage';
    await publisher.toConnection('c', 'e', 1);
    expect((publisher as any).clients.size).toBe(2);
  });

  it('turns a 410 into ConnectionGoneError, whichever way the SDK reports it', async () => {
    const publisher = new ApiGatewayPublisher(new InMemoryConnectionStore());
    stubSend(async () => { throw gone(); });
    await expect(publisher.toConnection('a', 'e', 1)).rejects.toBeInstanceOf(ConnectionGoneError);
    stubSend(async () => { throw Object.assign(new Error('gone'), { name: 'GoneException' }); });
    await expect(publisher.toConnectionRaw('a', 1)).rejects.toBeInstanceOf(ConnectionGoneError);
  });

  it('propagates any other failure', async () => {
    stubSend(async () => { throw new Error('throttled'); });
    await expect(new ApiGatewayPublisher(new InMemoryConnectionStore()).toConnection('a', 'e', 1)).rejects.toThrow('throttled');
  });

  it('closes a connection with DeleteConnection, already-gone included', async () => {
    const send = stubSend();
    const publisher = new ApiGatewayPublisher(new InMemoryConnectionStore());
    await publisher.disconnect('a');
    const [command] = send.mock.calls[0] as any[];
    expect(command.constructor.name).toBe('DeleteConnectionCommand');
    expect(command.input).toEqual({ ConnectionId: 'a' });

    stubSend(async () => { throw gone(); });
    await expect(publisher.disconnect('a')).resolves.toBeUndefined();
    stubSend(async () => { throw Object.assign(new Error('x'), { name: 'GoneException' }); });
    await expect(publisher.disconnect('a')).resolves.toBeUndefined();
    stubSend(async () => { throw new Error('denied'); });
    await expect(publisher.disconnect('a')).rejects.toThrow('denied');
  });

  it('fans out to a room immediately, reaping the gone and shrugging off the rest', async () => {
    const store = new InMemoryConnectionStore();
    for (const id of ['ok', 'ghost', 'broken']) {
      await store.add(id, { connectedAt: 1 });
      await store.join(id, 'r');
    }
    const delivered: string[] = [];
    stubSend(async (command: any) => {
      if (command.input.ConnectionId === 'ghost') throw gone();
      if (command.input.ConnectionId === 'broken') throw new Error('boom');
      delivered.push(command.input.ConnectionId);
    });
    await new ApiGatewayPublisher(store).toRoom('r', 'news', 1);
    expect(delivered).toEqual(['ok']);
    expect(await store.membersOf('r')).toEqual(['ok', 'broken']);
  });
});
