/* Test doubles for the ports, and a bridge assembled from them. */

import {
  ConnectHook,
  ConnectionStore,
  GatewayBridge,
  GatewayClient,
  InMemoryConnectionStore,
  ProtocolHandler,
  RealtimePublisher,
  SessionMeta,
} from '../../src';

/** An in-memory store that counts reads — rehydration is meant to be rare. */
export class SpyStore implements ConnectionStore {
  readonly inner = new InMemoryConnectionStore();
  gets = 0;
  add(id: string, meta: SessionMeta) {
    return this.inner.add(id, meta);
  }
  remove(id: string) {
    return this.inner.remove(id);
  }
  join(id: string, room: string) {
    return this.inner.join(id, room);
  }
  leave(id: string, room: string) {
    return this.inner.leave(id, room);
  }
  membersOf(room: string) {
    return this.inner.membersOf(room);
  }
  pageMembersOf(room: string) {
    return this.inner.pageMembersOf(room);
  }
  async get(id: string) {
    this.gets += 1;
    return this.inner.get(id);
  }
}

/** A store with only the required members — no pageMembersOf(). */
export function minimalStore(members: string[] = []): ConnectionStore {
  return {
    add: async () => {},
    remove: async () => {},
    join: async () => {},
    leave: async () => {},
    membersOf: async () => members,
    get: async () => null,
  };
}

/** A store written for 2.x, without get() — it only reaches the bridge through
 *  plain JavaScript or a cast, which is what the runtime guard is for. */
export function storeFrom2x(): ConnectionStore {
  const { get: _get, ...rest } = minimalStore();
  return rest as unknown as ConnectionStore;
}

export interface SentFrame {
  id: string;
  event: string;
  data: unknown;
}

/** A publisher that records instead of delivering. */
export class SpyPublisher implements RealtimePublisher {
  readonly sent: SentFrame[] = [];
  readonly raw: Array<{ id: string; payload: any }> = [];
  readonly disconnected: string[] = [];
  async toConnection(id: string, event: string, data: unknown) {
    this.sent.push({ id, event, data });
  }
  async toConnectionRaw(id: string, payload: unknown) {
    this.raw.push({ id, payload });
  }
  async disconnect(id: string) {
    this.disconnected.push(id);
  }
  /** Frames sent to one connection, by event name. */
  to(id: string, event?: string) {
    return this.sent.filter(f => f.id === id && (!event || f.event === event));
  }
}

export interface Recorded {
  frame: unknown;
  client: GatewayClient;
}

/** A protocol that claims every frame and remembers what it saw. */
export function recorder(name = 'recorder') {
  const seen: Recorded[] = [];
  const disconnects: Array<{ id: string; client?: GatewayClient; reason?: string }> = [];
  const protocol: ProtocolHandler = {
    name,
    handleFrame: async (frame, client) => {
      seen.push({ frame, client });
      return true;
    },
    onDisconnect: async (id, client, reason) => {
      disconnects.push({ id, client, reason });
    },
  };
  return { seen, disconnects, protocol };
}

export interface BridgeOptions {
  store?: ConnectionStore;
  publisher?: RealtimePublisher;
  hooks?: ConnectHook[];
  protocols?: ProtocolHandler[];
  connectTimeout?: number;
  maxConnectionData?: number;
}

/** A local bridge over spy backends. */
export function bridgeWith(options: BridgeOptions = {}) {
  const store = options.store ?? new SpyStore();
  const publisher = options.publisher ?? new SpyPublisher();
  let builder = GatewayBridge.builder().provider('local').store(store).publisher(publisher);
  for (const protocol of options.protocols ?? []) builder = builder.use(protocol);
  for (const hook of options.hooks ?? []) builder = builder.onConnect(hook);
  if (options.connectTimeout) builder = builder.connectTimeout(options.connectTimeout);
  if (options.maxConnectionData) builder = builder.maxConnectionData(options.maxConnectionData);
  return { bridge: builder.build(), store, publisher };
}

/** Resolve after the current macrotask — lets fire-and-forget work land. */
export const tick = () => new Promise(resolve => setImmediate(resolve));

export const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
