import { afterEach, describe, expect, it, vi } from 'vitest';
import { InMemoryConnectionStore, LocalPublisher, LocalSocketRegistry } from '../../../src';

afterEach(() => LocalSocketRegistry.clear());

describe('InMemoryConnectionStore', () => {
  it('reads a connection back, or null', async () => {
    const store = new InMemoryConnectionStore();
    await store.add('a', { connectedAt: 1, data: { user: 'ada' } });
    expect(await store.get('a')).toEqual({ connectedAt: 1, data: { user: 'ada' } });
    expect(await store.get('nobody')).toBeNull();
  });

  it('isolates the stored copy from both the writer and the reader', async () => {
    const store = new InMemoryConnectionStore();
    const meta = { connectedAt: 1, data: { user: 'ada' } };
    await store.add('a', meta);
    meta.data.user = 'mallory';
    const read = await store.get('a');
    read!.data!.user = 'eve';
    expect((await store.get('a'))!.data).toEqual({ user: 'ada' });
  });

  it('keeps room membership, and forgets it on remove', async () => {
    const store = new InMemoryConnectionStore();
    await store.add('a', { connectedAt: 1 });
    await store.join('a', 'r1');
    await store.join('a', 'r2');
    await store.join('b', 'r1');
    expect(await store.membersOf('r1')).toEqual(['a', 'b']);
    await store.leave('b', 'r1');
    expect(await store.membersOf('r1')).toEqual(['a']);
    await store.remove('a');
    expect(await store.membersOf('r1')).toEqual([]);
    expect(await store.membersOf('r2')).toEqual([]);
    expect(await store.get('a')).toBeNull();
  });

  it('answers an unknown room with nobody, in one page', async () => {
    const store = new InMemoryConnectionStore();
    expect(await store.membersOf('empty')).toEqual([]);
    await store.leave('a', 'empty');
    await store.join('a', 'r');
    expect(await store.pageMembersOf('r')).toEqual({ items: ['a'], cursor: undefined });
  });
});

describe('LocalPublisher', () => {
  const socket = () => ({ send: vi.fn(), close: vi.fn() });

  it('delivers { event, data } frames to the registered socket', async () => {
    const ws = socket();
    LocalSocketRegistry.set('a', ws);
    await new LocalPublisher(new InMemoryConnectionStore()).toConnection('a', 'hello', { n: 1 });
    expect(ws.send).toHaveBeenCalledWith(JSON.stringify({ event: 'hello', data: { n: 1 } }));
  });

  it('delivers raw payloads verbatim', async () => {
    const ws = socket();
    LocalSocketRegistry.set('a', ws);
    await new LocalPublisher(new InMemoryConnectionStore()).toConnectionRaw('a', { type: 'next', id: '1' });
    expect(ws.send).toHaveBeenCalledWith(JSON.stringify({ type: 'next', id: '1' }));
  });

  it('logs a push for a connection with no socket, labelled by event or type', async () => {
    const log = vi.spyOn(console, 'log').mockImplementation(() => {});
    const publisher = new LocalPublisher(new InMemoryConnectionStore());
    await publisher.toConnection('ghost', 'hello', 1);
    await publisher.toConnectionRaw('ghost', { type: 'ping' });
    await publisher.toConnectionRaw('ghost', 'plain');
    expect(log.mock.calls.map(c => c[0])).toEqual([
      '[push -> ghost] hello',
      '[push -> ghost] ping',
      '[push -> ghost] raw',
    ]);
  });

  it('fans out to a room immediately', async () => {
    const store = new InMemoryConnectionStore();
    const [a, b] = [socket(), socket()];
    LocalSocketRegistry.set('a', a);
    LocalSocketRegistry.set('b', b);
    await store.join('a', 'r');
    await store.join('b', 'r');
    await new LocalPublisher(store).toRoom('r', 'news', 1);
    expect(a.send).toHaveBeenCalledOnce();
    expect(b.send).toHaveBeenCalledOnce();
  });

  it('closes the socket on disconnect, and tolerates one that is gone or has no close()', async () => {
    const ws = socket();
    LocalSocketRegistry.set('a', ws);
    LocalSocketRegistry.set('b', { send: vi.fn() });
    const publisher = new LocalPublisher(new InMemoryConnectionStore());
    await publisher.disconnect('a');
    await publisher.disconnect('b');
    await publisher.disconnect('nobody');
    expect(ws.close).toHaveBeenCalledOnce();
  });
});
