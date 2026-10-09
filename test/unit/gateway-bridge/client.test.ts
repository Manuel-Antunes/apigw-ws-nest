import { describe, expect, it, vi } from 'vitest';
import {
  GLOBAL_ROOM,
  GatewayClient,
  GatewayServer,
  InMemoryConnectionStore,
  runInDispatchScope,
} from '../../../src';
import { SpyPublisher, minimalStore, sleep } from '../../helpers/fakes';

const make = (options?: ConstructorParameters<typeof GatewayClient>[3]) => {
  const store = new InMemoryConnectionStore();
  const publisher = new SpyPublisher();
  return { client: new GatewayClient('c1', store, publisher, options), store, publisher };
};

describe('GatewayClient', () => {
  it('starts open, with an empty identity and a minimal handshake', () => {
    const { client } = make();
    expect(client.phase).toBe('open');
    expect(client.data).toEqual({});
    expect(client.rehydrated).toBe(false);
    expect(client.handshake).toEqual({
      headers: {}, query: {}, subprotocols: [], connectedAt: expect.any(Number),
    });
    client.data = { replaced: true }; // writable when no connect phase made it
    expect(client.data).toEqual({ replaced: true });
  });

  it('takes a handshake, data and the rehydrated mark from its options', () => {
    const handshake = { headers: {}, query: {}, subprotocols: [], connectedAt: 7 };
    const { client } = make({ handshake, data: { user: 'ada' }, rehydrated: true });
    expect(client.handshake).toBe(handshake);
    expect(client.data).toEqual({ user: 'ada' });
    expect(client.rehydrated).toBe(true);
  });

  it('sends { event, data } frames and raw payloads to its own connection', async () => {
    const { client, publisher } = make();
    await client.send({ event: 'a', data: 1 });
    await client.emit('b', 2);
    await client.sendRaw({ type: 'next' });
    expect(publisher.sent).toEqual([
      { id: 'c1', event: 'a', data: 1 },
      { id: 'c1', event: 'b', data: 2 },
    ]);
    expect(publisher.raw).toEqual([{ id: 'c1', payload: { type: 'next' } }]);
  });

  it('makes an un-awaited send hold the current dispatch open', async () => {
    let delivered = false;
    const slow = new SpyPublisher();
    slow.toConnection = async () => {
      await sleep(20);
      delivered = true;
    };
    const client = new GatewayClient('c1', new InMemoryConnectionStore(), slow);
    await runInDispatchScope(async () => {
      void client.emit('fire', 'forget');
    });
    expect(delivered).toBe(true);
  });

  it('refuses a raw send when the publisher cannot', async () => {
    const client = new GatewayClient('c1', minimalStore(), { toConnection: async () => {} });
    await expect(client.sendRaw({})).rejects.toThrow(/does not implement toConnectionRaw/);
  });

  it('joins and leaves rooms through the store', async () => {
    const { client, store } = make();
    await client.join('r');
    expect(await store.membersOf('r')).toEqual(['c1']);
    await client.leave('r');
    expect(await store.membersOf('r')).toEqual([]);
  });

  describe('while $connect is being decided', () => {
    it('records joins and leaves instead of writing them', async () => {
      const { client, store } = make({ phase: 'connecting' });
      expect(client.phase).toBe('connecting');
      await client.join('r');
      await client.leave('r');
      expect(await store.membersOf('r')).toEqual([]);
    });

    it('throws on every kind of send', () => {
      const { client } = make({ phase: 'connecting' });
      expect(() => client.send({ event: 'e', data: 1 })).toThrow(/client\.send\(\) during \$connect/);
      expect(() => client.emit('e', 1)).toThrow(/client\.emit\(\) during \$connect/);
      expect(() => client.sendRaw(1)).toThrow(/client\.sendRaw\(\) during \$connect/);
    });

    it('treats disconnect() as a refusal', async () => {
      const kick = vi.fn(async () => {});
      const { client, publisher } = make({ phase: 'connecting', kick });
      await client.disconnect();
      expect(client.phase).toBe('refused');
      expect(kick).not.toHaveBeenCalled();
      expect(publisher.disconnected).toEqual([]);
    });
  });

  describe('disconnect() on an open connection', () => {
    it('uses the bridge\'s kick when it has one', async () => {
      const kick = vi.fn(async () => {});
      const { client, publisher } = make({ kick });
      await client.disconnect();
      expect(kick).toHaveBeenCalledOnce();
      expect(publisher.disconnected).toEqual([]);
    });

    it('otherwise asks the publisher', async () => {
      const { client, publisher } = make();
      await client.disconnect();
      expect(publisher.disconnected).toEqual(['c1']);
    });

    it('fails when the publisher cannot close sockets', async () => {
      const client = new GatewayClient('c1', minimalStore(), { toConnection: async () => {} });
      await expect(client.disconnect()).rejects.toThrow(/does not implement disconnect/);
    });
  });
});

describe('GatewayServer', () => {
  const server = () => {
    const published: unknown[] = [];
    return { published, server: new GatewayServer(() => ({ publish: async m => void published.push(m) })) };
  };

  it('records a room broadcast', async () => {
    const { server: s, published } = server();
    await s.to('orders').emit('updated', { id: 7 });
    expect(published).toEqual([{ kind: 'room', room: 'orders', event: 'updated', data: { id: 7 } }]);
  });

  it('records a global broadcast to the global room', async () => {
    const { server: s, published } = server();
    await s.emit('announcement', 'hi');
    expect(published).toEqual([{ kind: 'room', room: GLOBAL_ROOM, event: 'announcement', data: 'hi' }]);
  });

  it('keeps EventEmitter\'s own events in-process', () => {
    const { server: s, published } = server();
    const onConnection = vi.fn();
    const sym = Symbol('x');
    const onSymbol = vi.fn();
    s.on('connection', onConnection);
    s.on(sym, onSymbol);
    s.emit('connection', 'client');
    s.emit(sym, 1);
    expect(onConnection).toHaveBeenCalledWith('client');
    expect(onSymbol).toHaveBeenCalledWith(1);
    expect(published).toEqual([]);
  });

  it('drops its listeners on close', () => {
    const { server: s } = server();
    s.on('connection', () => {});
    s.close();
    expect(s.listenerCount('connection')).toBe(0);
  });
});
