/* =============================================================================
 *  ApiGatewayWsAdapter — the single transport plug-in.
 * =============================================================================
 *  It is BOTH halves of the integration:
 *    - a custom NestJS WebSocketAdapter (no port/server): the source of frames is
 *      the synthetic hub (fed by GatewayBridge) and the sink is the publisher, so
 *      the same @SubscribeMessage handlers run unchanged over API Gateway.
 *    - the inbound HTTP entry: it receives the HTTP adapter directly and registers
 *      the raw API Gateway dispatch route ON IT — outside the Nest controller
 *      lifecycle (like a middleware). No separate http-bridge module.
 *
 *  Body parsing: createNestApp constructs this BEFORE the caller runs
 *  init()/listen(), so the route sits ahead of Nest's global body-parser and 404
 *  handler in the Express stack. We therefore read+parse the JSON body ourselves
 *  (preferring an already-parsed req.body), which also keeps the library free of a
 *  direct `express` dependency.
 *
 *  THE ROUTE IS AN ENTRY POINT LIKE ANY OTHER, so it is OFF unless asked for.
 *  An app that serves its Nest HTTP app from Lambda (a function URL, Lambda Web
 *  Adapter) would expose it to the internet — and since identity is keyed by
 *  connectionId, a forged frame naming a live connection would be served as that
 *  connection. HTTP/ECS mode turns it on with `dispatchPath` and protects it with
 *  `dispatchSecret`; Lambda, which calls bridge.serve(), never needs it.
 *
 *  NOTE: streams are Lambda-only by design — there is no HTTP /stream route.
 * ========================================================================== */

import { HttpServer, INestApplication, WebSocketAdapter } from "@nestjs/common";
import { DiscoveryService, ModulesContainer } from "@nestjs/core";
import { createHash, timingSafeEqual } from "crypto";
import { EventEmitter } from "events";
import { Observable, isObservable } from "rxjs";
import { filter } from "rxjs/operators";
import { DISPATCH_SECRET, DISPATCH_SECRET_HEADER } from "./config";
import { ApiGwWsEvent, ClientFrame } from "./contract";
import {
  BoundHandler,
  GatewayBridge,
  GatewayClient,
  ProtocolHandler,
  isAnnouncing,
} from "./gateway-bridge";
import { enqueueBroadcast } from "./dispatch-scope";

/** `@WebSocketGateway()`'s marker — the key Nest's own SocketModule looks for
 *  (the same in Nest 11 and 12). */
const GATEWAY_METADATA = "websockets:is_gateway";

/** One gateway's lifecycle hooks, as the adapter found them at boot. */
export interface GatewayLifecycle {
  name: string;
  connect?: (client: GatewayClient) => unknown;
  disconnect?: (client: GatewayClient, reason: string) => unknown;
}

/**
 * The `{ event, data }` protocol, as a plug-in like any other.
 *
 * It is the FALLBACK: unlike graphql-transport-ws it has no frame signature to
 * recognise (any JSON object with an `event` could be one), so it must only see
 * what no signature-bearing protocol claimed. Routing itself lives on the client
 * (`handleFrame`, installed by bindMessageHandlers below) because Nest binds
 * handlers per connection, not per bridge.
 *
 * Registering it here rather than inside the bridge is deliberate: a bridge with
 * no Nest adapter genuinely has no `{ event, data }` handling, and now says so.
 */
export class NestGatewayProtocol implements ProtocolHandler {
  readonly name = "nest";
  readonly fallback = true;
  /** Present only under `lifecycle: 'connect'`. Their absence is what keeps the
   *  bridge on its unchanged $connect path otherwise. */
  readonly onConnect?: (client: GatewayClient) => Promise<void>;
  readonly onDisconnect?: (
    connectionId: string,
    client?: GatewayClient,
    reason?: string,
  ) => Promise<void>;

  /** `gateways` turns the lifecycle hooks on. The adapter passes an array it
   *  fills at boot, once Nest has instantiated the gateways. */
  constructor(gateways?: GatewayLifecycle[]) {
    if (!gateways) return;
    // Every gateway's handleConnection, awaited, in the order Nest binds them;
    // the first throw refuses the connection and stops the rest.
    this.onConnect = async client => {
      for (const gateway of gateways) {
        if (!gateway.connect) continue;
        await gateway.connect(client);
        if (client.phase === "refused") return; // client.disconnect(): stop, like a throw
      }
    };
    // A disconnect cannot be refused, so one gateway's failure is logged and the
    // others still run.
    this.onDisconnect = async (_connectionId, client, reason = "disconnect") => {
      if (!client) return; // never accepted here, or already cleaned up
      for (const gateway of gateways) {
        if (!gateway.disconnect) continue;
        try {
          await gateway.disconnect(client, reason);
        } catch (err) {
          // eslint-disable-next-line no-console
          console.error(`[disconnect: ${gateway.name}]`, err);
        }
      }
    };
  }

  async handleFrame(frame: unknown, client: GatewayClient): Promise<boolean> {
    // No handlers bound yet (no @WebSocketGateway in the app) — let the frame go
    // unclaimed rather than silently swallowing it.
    if (!client.handleFrame) return false;
    await client.handleFrame(frame as ClientFrame);
    return true;
  }
}

export interface ApiGatewayWsAdapterOptions {
  /** Register the HTTP dispatch route — the path API Gateway's HTTP integration
   *  POSTs WebSocket events to (HTTP/ECS mode). Unset or `false`: no route,
   *  which is what Lambda wants. `DISPATCH_PATH` is the conventional value
   *  (APIGW_DISPATCH_PATH, else '/@dispatch'). */
  dispatchPath?: string | false;
  /** Required value of `dispatchSecretHeader` on the dispatch route; anything
   *  else is answered 403 before the body is read. Default
   *  APIGW_DISPATCH_SECRET. */
  dispatchSecret?: string;
  /** Default 'x-apigw-dispatch-secret'. */
  dispatchSecretHeader?: string;
  /**
   * How the gateways' OnGatewayConnection / OnGatewayDisconnect run.
   *
   *  - 'connect' (the default): handleConnection runs ONCE, at $connect,
   *    awaited, with `client.handshake` — throwing (or client.disconnect())
   *    refuses the socket, and what it writes to `client.data` is persisted
   *    with the connection. handleDisconnect(client, reason) runs on
   *    $disconnect, a 410 Gone, or bridge.disconnect().
   *  - 'legacy': the 2.x behaviour, an opt-out while migrating — handleConnection
   *    runs lazily, fire-and-forget, on the first frame each instance sees, with
   *    no handshake; handleDisconnect never runs; nothing is read from the store.
   */
  lifecycle?: "legacy" | "connect";
}

/** The open-route warning is about the process, not each adapter. */
let warnedOpenDispatchRoute = false;

const sha256 = (value: string) => createHash("sha256").update(value).digest();

export class ApiGatewayWsAdapter implements WebSocketAdapter {
  httpAdapter: HttpServer;
  private readonly lifecycle: "legacy" | "connect";
  /** Filled on the first create() under lifecycle 'connect'. */
  private readonly gateways: GatewayLifecycle[] = [];
  private lifecycleTaken = false;

  constructor(
    protected readonly app: INestApplication,
    private readonly bridge: GatewayBridge,
    options: ApiGatewayWsAdapterOptions = {},
  ) {
    this.httpAdapter = app.getHttpAdapter();
    this.lifecycle = options.lifecycle ?? "connect";
    // Under 'connect' the protocol carries onConnect, and use() refuses it right
    // here — at boot — when the store cannot rehydrate client.data.
    this.bridge.use(
      new NestGatewayProtocol(this.lifecycle === "connect" ? this.gateways : undefined),
    );

    const path = options.dispatchPath;
    if (!path) return;
    const secret = options.dispatchSecret ?? DISPATCH_SECRET;
    this.registerDispatchRoute(
      path,
      secret || undefined,
      options.dispatchSecretHeader ?? DISPATCH_SECRET_HEADER,
    );
    if (!secret && this.bridge.hasConnectHooks && !warnedOpenDispatchRoute) {
      warnedOpenDispatchRoute = true;
      // eslint-disable-next-line no-console
      console.warn(
        `[ApiGatewayWsAdapter] POST ${path} is registered without a dispatchSecret while connect ` +
          `hooks are active: whoever can reach it can forge a frame for a live connectionId and be ` +
          `served as that connection. Set dispatchSecret (APIGW_DISPATCH_SECRET), and drop ` +
          `dispatchPath wherever the route is not needed (Lambda).`,
      );
    }
  }

  /* ---- inbound: API Gateway HTTP integration, registered on the adapter ---- */

  private registerDispatchRoute(path: string, secret: string | undefined, header: string) {
    // Compared as SHA-256 digests: equal lengths for timingSafeEqual, and no
    // length leak about the secret.
    const expected = secret ? sha256(secret) : undefined;
    const headerName = header.toLowerCase();
    this.httpAdapter.post(path, async (req: any, res: any) => {
      if (expected) {
        const given = req.headers?.[headerName];
        if (typeof given !== "string" || !timingSafeEqual(sha256(given), expected)) {
          this.httpAdapter.reply(res, "forbidden", 403);
          return;
        }
      }
      let event: ApiGwWsEvent;
      try {
        event = (await this.readJsonBody(req)) as ApiGwWsEvent;
      } catch {
        this.httpAdapter.reply(res, "invalid JSON body", 400);
        return;
      }
      const result = await this.bridge.dispatch(event);
      // Through the HTTP adapter, so Express and Fastify behave the same — and
      // so the $connect Sec-WebSocket-Protocol echo actually leaves the process.
      for (const [name, value] of Object.entries(result.headers ?? {})) {
        this.httpAdapter.setHeader(res, name, value);
      }
      this.httpAdapter.reply(res, result.body ?? "", result.statusCode);
    });
  }

  /** Read a JSON body from a raw Node/Express request without depending on a
   *  particular body-parser being mounted. */
  private readJsonBody(req: any): Promise<any> {
    if (req.body !== undefined && req.body !== null)
      return Promise.resolve(req.body);
    return new Promise((resolve, reject) => {
      let data = "";
      req.on("data", (chunk: any) => (data += chunk));
      req.on("end", () => {
        try {
          resolve(data ? JSON.parse(data) : {});
        } catch (err) {
          reject(err);
        }
      });
      req.on("error", reject);
    });
  }

  /* ---- outbound/WS pipeline: the NestJS WebSocketAdapter contract ---------- */

  // No port: hand Nest the synthetic server (also the @WebSocketServer() value
  // and the 'connection' hub).
  create(_port: number, _options: any = {}): any {
    // Nest calls this during init(), with every provider already instantiated
    // and before it subscribes any gateway's hooks — the one moment to take the
    // hooks over. Later gateways reuse the cached server.
    if (this.lifecycle === "connect" && !this.lifecycleTaken) {
      this.lifecycleTaken = true;
      this.takeOverLifecycleHooks();
    }
    return this.bridge.server;
  }

  /**
   * Find the gateways the way Nest's SocketModule does (providers whose class
   * carries `@WebSocketGateway()`'s metadata, in module then provider order),
   * and take their two lifecycle hooks over:
   *
   *  - handleConnection: Nest calls it fire-and-forget whenever the hub emits
   *    'connection' — which the bridge must keep emitting, because the same Nest
   *    callback binds the gateway's message handlers. So the original is kept
   *    for the awaited call at $connect, and an own-property shim on the
   *    instance does nothing when Nest calls it for a client the bridge is
   *    announcing. Nest 11 looks the method up at call time and Nest 12 binds it
   *    after create(), so either way Nest gets the shim. Anyone else calling the
   *    method gets the original behaviour.
   *  - handleDisconnect: Nest only wires it through bindClientDisconnect, which
   *    this adapter does not implement, so it is simply called from cleanup.
   */
  private takeOverLifecycleHooks() {
    const seen = new Set<unknown>();
    const discovery = new DiscoveryService(this.app.get(ModulesContainer));
    for (const wrapper of discovery.getProviders()) {
      const { metatype, instance } = wrapper as { metatype: any; instance: any };
      if (typeof metatype !== "function" || !Reflect.getMetadata(GATEWAY_METADATA, metatype)) {
        continue;
      }
      // The instance, not just the prototype: a hook may be an arrow-function
      // class field.
      const declared = instance ?? metatype.prototype ?? {};
      const hasConnect = typeof declared.handleConnection === "function";
      const hasDisconnect = typeof declared.handleDisconnect === "function";
      if (!hasConnect && !hasDisconnect) continue;
      const name: string = metatype.name || "gateway";
      if (!wrapper.isDependencyTreeStatic()) {
        // A request-scoped gateway gets a fresh instance per context, whose
        // handleConnection the shim cannot reach.
        throw new Error(
          `${name} is request-scoped and implements handleConnection/handleDisconnect, which ` +
            `ApiGatewayWsAdapter's lifecycle: 'connect' cannot run. Make the gateway a default-scoped ` +
            `provider, or use lifecycle: 'legacy'.`,
        );
      }
      if (!instance || seen.has(instance)) continue;
      seen.add(instance);

      const handleConnection: Function | undefined = instance.handleConnection;
      const handleDisconnect: Function | undefined = instance.handleDisconnect;
      if (handleConnection) {
        Object.defineProperty(instance, "handleConnection", {
          configurable: true,
          writable: true,
          value(client: unknown, ...rest: unknown[]) {
            // Nest's call while the bridge announces the client: the real one
            // already ran, awaited, at $connect.
            if (client && typeof client === "object" && isAnnouncing(client)) return;
            return handleConnection.call(instance, client, ...rest);
          },
        });
      }
      this.gateways.push({
        name,
        connect: handleConnection && (client => handleConnection.call(instance, client)),
        disconnect:
          handleDisconnect && ((client, reason) => handleDisconnect.call(instance, client, reason)),
      });
    }
  }

  bindClientConnect(
    hub: EventEmitter,
    callback: (client: GatewayClient) => void,
  ) {
    hub.on("connection", callback);
  }

  bindMessageHandlers(
    client: GatewayClient,
    handlers: BoundHandler[],
    _process: (data: any) => Observable<any>,
  ) {
    // Nest calls this ONCE PER @WebSocketGateway for the same connection. Merge
    // each gateway's routes into the client's handler map instead of overwriting,
    // so every gateway stays reachable — not just the last one bound.
    for (const h of handlers) client.handlers.set(h.message, h);

    // Install the AWAITABLE processor exactly once. dispatch() awaits it so the
    // Lambda stays warm until the handler ran, its @Ack/return was delivered, and
    // any room broadcasts were enqueued. It resolves routes from client.handlers,
    // so it sees handlers added by gateways bound after this point too.
    if (client.handleFrame) return;
    client.handleFrame = async (frame: ClientFrame) => {
      const handler = client.handlers.get(frame.event);
      if (!handler) return;

      // @Ack() — an immediate acknowledgement: a function returning a WsResponse,
      // sent back to THIS client. Enqueued so dispatch flushes it before freeze.
      const ack = (response: ClientFrame) => {
        enqueueBroadcast(
          client.send(response).catch(e => this.bridge.onSendError(client, e)),
        );
      };

      // Nest pre-binds the client as args[0]; we pass (data, ack). The result is
      // always a Promise (the WsProxy wraps handlers as async).
      const result = await handler.callback(frame.data, ack);

      // A) Observable<WsResponse> => a LIVE per-client stream. It may never
      //    complete (a BehaviorSubject), so we do NOT await completion: keep the
      //    subscription alive across warm invocations and enqueue each emission's
      //    send so whichever dispatch triggered it (e.g. another client's
      //    post.create -> subject.next) flushes it before the Lambda freezes.
      if (isObservable(result)) {
        const sub = result.pipe(filter(r => r != null)).subscribe({
          next: r =>
            enqueueBroadcast(
              client.send(r as ClientFrame).catch(e => this.bridge.onSendError(client, e)),
            ),
          error: e => this.bridge.onSendError(client, e),
        });
        client.subscriptions.push(sub);
        return;
      }
      // B) Plain return => the response, UNLESS the handler already acked via @Ack.
      if (result != null && !handler.isAckHandledManually) {
        await client
          .send(result as ClientFrame)
          .catch(e => this.bridge.onSendError(client, e));
      }
    };
  }

  close() {
    /* nothing to tear down — API Gateway owns the sockets */
  }

  // Nest 11's SocketModule.close() calls adapter.dispose() during app shutdown
  // (app.close()). We hold no sockets/servers of our own, so this is a no-op —
  // but it MUST exist, or graceful shutdown throws "adapter.dispose is not a
  // function".
  dispose() {
    /* nothing to dispose — API Gateway owns the sockets */
  }
}
