/* =============================================================================
 *  The adapter inside a real Nest app — on Express and on Fastify.
 * =============================================================================
 *    - POST /@dispatch: off by default, on with dispatchPath, guarded by
 *      dispatchSecret, forwarding the $connect Sec-WebSocket-Protocol echo
 *    - lifecycle 'connect' (the default): handleConnection runs ONCE, at
 *      $connect, awaited, and its throw is the $connect status;
 *      handleDisconnect runs on $disconnect; a request-scoped gateway with
 *      those hooks fails at boot
 *    - lifecycle 'legacy': the 2.x behaviour, kept as an opt-out
 * ========================================================================== */

import {
  CanActivate,
  ExecutionContext,
  Injectable,
  Module,
  Scope,
  UnauthorizedException,
  UseGuards,
} from '@nestjs/common';
import {
  ConnectedSocket,
  MessageBody,
  OnGatewayConnection,
  OnGatewayDisconnect,
  SubscribeMessage,
  WebSocketGateway,
  WebSocketServer,
} from '@nestjs/websockets';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { DISPATCH_PATH, GatewayBridge, GatewayClient, GatewayServer, createNestApp } from '../../src';
import { connectEvent, disconnectEvent, frameEvent, nextId } from '../helpers/events';
import { SpyPublisher } from '../helpers/fakes';
import { Booted, PLATFORMS, boot, httpAdapterFor } from '../helpers/nest';

let booted: Booted | undefined;
afterEach(async () => {
  await booted?.app.close();
  booted = undefined;
});
beforeEach(() => {
  vi.spyOn(console, 'warn').mockImplementation(() => {});
  vi.spyOn(console, 'error').mockImplementation(() => {});
});

@Module({})
class EmptyModule {}

/* ---- a gateway that authenticates at $connect ---------------------------- */

const calls = { connect: [] as GatewayClient[], disconnect: [] as Array<{ data: unknown; reason: string }> };

@Injectable()
class UserGuard implements CanActivate {
  canActivate(context: ExecutionContext) {
    return !!context.switchToWs().getClient<GatewayClient>().data.user;
  }
}

@WebSocketGateway()
class AuthGateway implements OnGatewayConnection, OnGatewayDisconnect {
  @WebSocketServer() server!: GatewayServer;

  async handleConnection(client: GatewayClient<{ user?: string }>) {
    calls.connect.push(client);
    const token = client.handshake.query.token;
    if (!token) return;
    if (token !== 'good') throw new UnauthorizedException('bad token');
    client.data.user = 'ada';
    await client.join('user:ada');
  }

  handleDisconnect(client: GatewayClient, reason?: string) {
    calls.disconnect.push({ data: client.data, reason: reason! });
  }

  @UseGuards(UserGuard)
  @SubscribeMessage('whoami')
  whoami(@ConnectedSocket() client: GatewayClient<{ user?: string }>) {
    return { event: 'whoami', data: client.data.user };
  }

  @SubscribeMessage('echo')
  echo(@MessageBody() data: unknown) {
    return { event: 'echo', data };
  }

  @SubscribeMessage('shout')
  async shout(@MessageBody() data: { room: string }) {
    await this.server.to(data.room).emit('shouted', data);
  }
}

/** A second gateway on the same connection, whose hook is an arrow field. */
@WebSocketGateway()
class AuditGateway {
  seen = 0;
  handleConnection = async () => {
    this.seen += 1;
  };
}

@Module({ providers: [AuthGateway, AuditGateway, UserGuard] })
class AuthModule {}

beforeEach(() => {
  calls.connect.length = 0;
  calls.disconnect.length = 0;
});

describe.each(PLATFORMS)('on %s', platform => {
  describe('the dispatch route', () => {
    it('is absent by default', async () => {
      booted = await boot(EmptyModule, { platform });
      expect((await booted.post('/@dispatch', connectEvent(nextId()))).status).toBe(404);
    });

    it('is registered on DISPATCH_PATH and reaches the bridge', async () => {
      booted = await boot(EmptyModule, { platform, adapter: { dispatchPath: DISPATCH_PATH } });
      const id = nextId();
      const res = await booted.post('/@dispatch', connectEvent(id));
      expect(res.status).toBe(200);
      expect(await booted.bridge.store.get!(id)).not.toBeNull();
    });

    it('is absent with dispatchPath: false', async () => {
      booted = await boot(EmptyModule, { platform, adapter: { dispatchPath: false } });
      expect((await booted.post('/@dispatch', connectEvent(nextId()))).status).toBe(404);
    });

    it('can live on another path', async () => {
      booted = await boot(EmptyModule, { platform, adapter: { dispatchPath: '/ws-events' } });
      expect((await booted.post('/ws-events', connectEvent(nextId()))).status).toBe(200);
    });

    it('requires the secret, and forwards the subprotocol echo', async () => {
      booted = await boot(EmptyModule, {
        platform,
        adapter: { dispatchPath: DISPATCH_PATH, dispatchSecret: 's' },
        configure: b => b.subprotocols('graphql-transport-ws'),
      });
      const id = nextId();
      const event = connectEvent(id, { headers: { 'Sec-WebSocket-Protocol': 'graphql-transport-ws' } });
      expect((await booted.post('/@dispatch', event)).status).toBe(403);
      expect((await booted.post('/@dispatch', event, { 'x-apigw-dispatch-secret': 'nope' })).status).toBe(403);
      expect(await booted.bridge.store.get!(id)).toBeNull();
      const right = await booted.post('/@dispatch', event, { 'x-apigw-dispatch-secret': 's' });
      expect(right.status).toBe(200);
      expect(right.headers.get('sec-websocket-protocol')).toBe('graphql-transport-ws');
    });

    it('answers a frame through the gateway', async () => {
      const publisher = new SpyPublisher();
      booted = await boot(AuthModule, {
        platform,
        adapter: { dispatchPath: DISPATCH_PATH },
        configure: b => b.publisher(publisher),
      });
      const id = nextId();
      await booted.post('/@dispatch', connectEvent(id));
      const res = await booted.post('/@dispatch', frameEvent(id, { event: 'echo', data: { n: 1 } }));
      expect(res.status).toBe(200);
      expect(publisher.to(id, 'echo')).toEqual([{ id, event: 'echo', data: { n: 1 } }]);
    });
  });

  it('runs on the requested HTTP platform', async () => {
    booted = await boot(EmptyModule, { platform });
    expect(booted.app.getHttpAdapter().getType()).toBe(platform);
  });
});

describe('lifecycle: \'connect\' (the default)', () => {
  async function connectApp() {
    const publisher = new SpyPublisher();
    booted = await boot(AuthModule, { configure: b => b.publisher(publisher) });
    return { bridge: booted.bridge, publisher, app: booted.app };
  }

  it('turns handleConnection\'s throw into the $connect status', async () => {
    const { bridge } = await connectApp();
    const id = nextId();
    expect(await bridge.dispatch(connectEvent(id, { queryStringParameters: { token: 'bad' } }))).toEqual({
      statusCode: 401, body: 'bad token',
    });
    expect(await bridge.store.get!(id)).toBeNull();
  });

  it('persists what handleConnection wrote, joins included', async () => {
    const { bridge } = await connectApp();
    const id = nextId();
    expect((await bridge.dispatch(connectEvent(id, { queryStringParameters: { token: 'good' } }))).statusCode).toBe(200);
    expect((await bridge.store.get!(id))!.data).toEqual({ user: 'ada' });
    expect(await bridge.store.membersOf('user:ada')).toContain(id);
  });

  it('runs every gateway\'s handleConnection once — Nest\'s own call is swallowed', async () => {
    const { bridge, app } = await connectApp();
    const audit = app.get(AuditGateway);
    const id = nextId();
    await bridge.dispatch(connectEvent(id, { queryStringParameters: { token: 'good' } }));
    await bridge.dispatch(frameEvent(id, { event: 'echo', data: 1 }));
    await bridge.dispatch(frameEvent(id, { event: 'echo', data: 2 }));
    expect(calls.connect).toHaveLength(1);
    expect(audit.seen).toBe(1);
  });

  it('lets a guard read client.data, and refuse without it', async () => {
    const { bridge, publisher } = await connectApp();
    const [ada, anon] = [nextId(), nextId()];
    await bridge.dispatch(connectEvent(ada, { queryStringParameters: { token: 'good' } }));
    await bridge.dispatch(connectEvent(anon));
    await bridge.dispatch(frameEvent(ada, { event: 'whoami', data: {} }));
    await bridge.dispatch(frameEvent(anon, { event: 'whoami', data: {} }));
    expect(publisher.to(ada, 'whoami')[0].data).toBe('ada');
    expect(publisher.to(anon, 'exception')[0].data).toMatchObject({ message: 'Forbidden resource' });
  });

  it('rehydrates client.data for the guard on a new instance over the same store', async () => {
    const { bridge } = await connectApp();
    const id = nextId();
    await bridge.dispatch(connectEvent(id, { queryStringParameters: { token: 'good' } }));
    const publisher = new SpyPublisher();
    const second = GatewayBridge.builder().provider('local').store(bridge.store).publisher(publisher).build();
    const other = await createNestApp(AuthModule, second, { nest: { logger: false } });
    await other.init();
    try {
      await second.dispatch(frameEvent(id, { event: 'whoami', data: {} }));
      expect(publisher.to(id, 'whoami')[0].data).toBe('ada');
      expect(calls.connect).toHaveLength(1);
    } finally {
      await other.close();
    }
  });

  it('calls handleDisconnect with client.data and the reason', async () => {
    const { bridge } = await connectApp();
    const id = nextId();
    await bridge.dispatch(connectEvent(id, { queryStringParameters: { token: 'good' } }));
    await bridge.dispatch(disconnectEvent(id, 'bye'));
    expect(calls.disconnect).toEqual([{ data: { user: 'ada' }, reason: 'bye' }]);
  });

  it('still runs handleConnection for whoever calls it directly', async () => {
    const { app } = await connectApp();
    await app.get(AuthGateway).handleConnection({ handshake: { query: {} }, data: {} } as any);
    expect(calls.connect).toHaveLength(1);
  });

  it('routes room broadcasts from a gateway through the bridge', async () => {
    const { bridge, publisher } = await connectApp();
    const [a, b] = [nextId(), nextId()];
    await bridge.dispatch(connectEvent(a, { queryStringParameters: { token: 'good' } }));
    await bridge.dispatch(connectEvent(b));
    await bridge.dispatch(frameEvent(b, { event: 'shout', data: { room: 'user:ada' } }));
    expect(publisher.to(a, 'shouted')).toHaveLength(1);
    expect(publisher.to(b, 'shouted')).toHaveLength(0);
  });

  it('refuses at boot a request-scoped gateway with lifecycle hooks', async () => {
    @Injectable({ scope: Scope.REQUEST })
    @WebSocketGateway()
    class ScopedGateway {
      handleConnection() {}
      @SubscribeMessage('ping')
      ping() {
        return { event: 'pong', data: {} };
      }
    }
    @Module({ providers: [ScopedGateway] })
    class ScopedModule {}

    const bridge = GatewayBridge.builder().provider('local').build();
    const app = await createNestApp(ScopedModule, bridge, {
      nest: { logger: false, abortOnError: false },
    });
    await expect(app.init()).rejects.toThrow(/ScopedGateway is request-scoped/);
    await app.close().catch(() => {});
  });

  it('ignores gateways without lifecycle hooks, and providers that are not gateways', async () => {
    @WebSocketGateway()
    class PlainGateway {
      @SubscribeMessage('ping')
      ping() {
        return { event: 'pong', data: {} };
      }
    }
    @Injectable()
    class SomeService {
      handleConnection() {
        throw new Error('not a gateway — never called');
      }
    }
    @Module({ providers: [PlainGateway, SomeService] })
    class PlainModule {}

    const publisher = new SpyPublisher();
    booted = await boot(PlainModule, { configure: b => b.publisher(publisher) });
    const id = nextId();
    expect((await booted.bridge.dispatch(connectEvent(id))).statusCode).toBe(200);
    await booted.bridge.dispatch(frameEvent(id, { event: 'ping', data: {} }));
    expect(publisher.to(id, 'pong')).toHaveLength(1);
  });
});

describe('lifecycle: \'legacy\'', () => {
  it('keeps the 2.x behaviour exactly', async () => {
    booted = await boot(AuthModule, { adapter: { lifecycle: 'legacy' } });
    const { bridge } = booted;
    const id = nextId();
    expect((await bridge.dispatch(connectEvent(id, { queryStringParameters: { token: 'bad' } }))).statusCode).toBe(200);
    expect(calls.connect).toHaveLength(0);
    await bridge.dispatch(frameEvent(id, { event: 'echo', data: {} }));
    expect(calls.connect).toHaveLength(1); // lazily, on the first frame
    await bridge.dispatch(disconnectEvent(id));
    expect(calls.disconnect).toHaveLength(0);
  });
});

describe('createNestApp', () => {
  it('uses the default logger levels, and passes Nest options through', async () => {
    const bridge = GatewayBridge.builder().provider('local').build();
    const quiet = await createNestApp(EmptyModule, bridge);
    await quiet.init();
    await quiet.close();
    const app = await createNestApp(EmptyModule, GatewayBridge.builder().provider('local').build(), {
      httpAdapter: httpAdapterFor('fastify'),
      nest: { logger: false },
    });
    expect(app.getHttpAdapter().getType()).toBe('fastify');
    await app.close();
  });
});
