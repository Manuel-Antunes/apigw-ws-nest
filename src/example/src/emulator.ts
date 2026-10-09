/* =============================================================================
 *  Local "API Gateway WebSocket" emulator  (dev + tests)
 * =============================================================================
 *  Runs the REAL Lambda handler against REAL WebSockets, so the whole flow can be
 *  exercised without any AWS. It plays the role API Gateway plays in prod:
 *
 *    browser ws  --upgrade-->  this server  --CONNECT  event-->  handler()
 *                                 (non-2xx: the upgrade is refused, like API GW)
 *    browser msg --------->     this server  --MESSAGE  event-->  handler()
 *    handler push --------->    LocalPublisher --registry--> browser ws.send()
 *
 *  $connect is dispatched BEFORE the upgrade completes, so a connect hook that
 *  refuses (401, 403, ...) refuses the handshake itself — the client sees that
 *  status, and nothing is registered.
 *
 *  startEmulator() is what `pnpm dev` (local-server.ts) and the end-to-end tests
 *  both run; a test passes `port: 0` and gets its own isolated emulator.
 *
 *  RT_PROVIDER must be 'local' BEFORE the library is first imported (the example
 *  repositories pick their backend from it): local-server.ts sets it, and the
 *  e2e test project sets it in its environment.
 * ========================================================================== */

import * as http from "http";
import * as fs from "fs";
import * as path from "path";
import { AddressInfo } from "net";
import { Duplex } from "stream";
import { randomUUID } from "crypto";
import { WebSocket, WebSocketServer } from "ws";
import { INestApplication } from "@nestjs/common";
import {
  LocalSocketRegistry,
  InMemoryConnectionStore,
  ApiGwWsEvent,
  EVENT_TYPE,
  ROUTE,
  createNestApp,
  GatewayBridge,
} from "../..";
import {
  GRAPHQL_TRANSPORT_WS_PROTOCOL,
  InMemorySubscriptionRegistry,
  enableGraphQLSubscriptions,
} from "../../graphql";
import { AppModule } from "./app.module";
import { pubsub } from "./pubsub";

export interface EmulatorOptions {
  /** 0 = any free port. Default 6005. */
  port?: number;
  /** Log instance rebuilds. Default true. */
  verbose?: boolean;
  /** The Nest HTTP platform behind the app. Default 'express'. A factory, since
   *  every "Lambda instance" (reload) needs a fresh adapter. */
  platform?: "express" | "fastify";
}

export interface Emulator {
  port: number;
  /** ws://localhost:<port> */
  wsUrl: string;
  /** http://localhost:<port> */
  httpUrl: string;
  /** The Nest HTTP platform the current instance runs on. */
  httpPlatform(): string;
  /** The durable store every "instance" shares — DynamoDB's stand-in. */
  store: InMemoryConnectionStore;
  /** A brand-new Nest app + bridge over the same store: a new "Lambda instance". */
  reload(): Promise<number>;
  close(): Promise<void>;
}

const PUBLIC = path.join(__dirname, "..", "public");
const PAGES: Record<string, string> = {
  "/": "index.html",
  "/index.html": "index.html",
  "/chat": "chat.html",
  "/chat.html": "chat.html",
  "/graphql": "graphql.html",
  "/graphql.html": "graphql.html",
};

type Phase = "connect" | "disconnect" | "message";

/** What API Gateway knows about a connection and repeats on every event. */
interface ConnectionInfo {
  connectedAt: number;
  sourceIp?: string;
  userAgent?: string;
}

function gwEvent(
  connectionId: string,
  phase: Phase,
  info: ConnectionInfo,
  port: number,
  extra: {
    body?: string;
    headers?: Record<string, string | undefined>;
    query?: Record<string, string>;
    disconnect?: { code: number; reason: string };
  } = {},
): ApiGwWsEvent {
  const routeKey =
    phase === "connect"
      ? ROUTE.CONNECT
      : phase === "disconnect"
        ? ROUTE.DISCONNECT
        : ROUTE.DEFAULT;
  const eventType =
    phase === "connect"
      ? EVENT_TYPE.CONNECT
      : phase === "disconnect"
        ? EVENT_TYPE.DISCONNECT
        : EVENT_TYPE.MESSAGE;
  return {
    requestContext: {
      connectionId,
      routeKey,
      eventType,
      domainName: `localhost:${port}`,
      stage: "local",
      apiId: "local",
      requestId: randomUUID(),
      connectedAt: info.connectedAt,
      identity: { sourceIp: info.sourceIp, userAgent: info.userAgent },
      ...(phase === "message" ? { messageId: randomUUID() } : {}),
      ...(extra.disconnect
        ? {
            disconnectStatusCode: extra.disconnect.code,
            disconnectReason: extra.disconnect.reason,
          }
        : {}),
    },
    body: extra.body,
    // API Gateway forwards the handshake on $connect only — headers (that's
    // where Sec-WebSocket-Protocol comes from) and the query string.
    ...(phase === "connect"
      ? { headers: extra.headers, queryStringParameters: extra.query }
      : {}),
    isBase64Encoded: false,
  };
}

/** Where the subprotocol the bridge negotiated travels from the upgrade handler
 *  to ws's handleProtocols. */
const NEGOTIATED = Symbol("negotiated subprotocol");

/** Refuse an upgrade with an HTTP status, the way API Gateway answers a $connect
 *  integration that returned non-2xx. A `ws` client sees `unexpected-response`
 *  with that status; a browser sees a failed handshake. */
function refuseUpgrade(socket: Duplex, status: number, body = "") {
  socket.end(
    `HTTP/1.1 ${status} ${http.STATUS_CODES[status] ?? ""}\r\n` +
      "Connection: close\r\n" +
      "Content-Type: text/plain\r\n" +
      `Content-Length: ${Buffer.byteLength(body)}\r\n\r\n` +
      body,
  );
}

export async function startEmulator(options: EmulatorOptions = {}): Promise<Emulator> {
  const verbose = options.verbose ?? true;

  /* -------------------------------------------------------------------------
   *  "Lambda instance" lifecycle.
   *
   *  In production each warm Lambda container is one instance; a redeploy gives
   *  you a BRAND-NEW instance (empty in-process memory) while API Gateway keeps
   *  the existing WebSocket connections open and the durable state (connections,
   *  rooms, subscriptions) lives in DynamoDB.
   *
   *  reload() emulates exactly that: it throws away the old Nest app AND the old
   *  GatewayBridge — client map, rxjs Subjects, protocol handlers — while the
   *  two things standing in for DynamoDB are created ONCE, here, and handed to
   *  every new bridge. Delete either line and the cross-instance tests fail at
   *  once, which is exactly the failure a redeploy would cause in production.
   * ----------------------------------------------------------------------- */
  const store = new InMemoryConnectionStore(); // stands in for DynamoDB
  const registry = new InMemorySubscriptionRegistry(); // ...and so does this

  let bridge!: GatewayBridge;
  let app: INestApplication | undefined;
  let generation = 0;
  let port = options.port ?? 6005;

  async function reload(): Promise<number> {
    const previous = app;
    // A brand-new bridge over the SAME durable store — a faithful "new Lambda
    // instance" rather than a full restart. LocalSocketRegistry, which stands in
    // for API Gateway's @connections, is untouched, so live sockets survive.
    const next = GatewayBridge.builder().provider("local").store(store).build();
    // The adapter's defaults: $connect runs the gateways' handleConnection,
    // awaited, and there is no dispatch route — the emulator calls the bridge.
    const nextApp = await createNestApp(AppModule, next, {
      httpAdapter:
        options.platform === "fastify"
          ? new (require("@nestjs/platform-fastify").FastifyAdapter)()
          : undefined,
    });
    await nextApp.init();
    // Re-registered against the NEW bridge, with the SAME registry and PubSub —
    // so subscriptions taken out before the reload still deliver after it.
    enableGraphQLSubscriptions(nextApp, next, { registry, pubsub });
    bridge = next;
    app = nextApp;
    generation += 1;
    if (previous) await previous.close(); // the old instance goes once the new one is live
    if (verbose) {
      // eslint-disable-next-line no-console
      console.log(`[instance ${generation}] ready — connections + subscriptions preserved`);
    }
    return generation;
  }

  /** Feed an API Gateway event into the CURRENT instance's bridge. */
  const dispatch = (event: ApiGwWsEvent) => bridge.dispatch(event);

  const server = http.createServer((req, res) => {
    const page = PAGES[(req.url ?? "/").split("?")[0]];
    if (req.method === "GET" && page) {
      res.writeHead(200, { "content-type": "text/html; charset=utf-8" });
      res.end(fs.readFileSync(path.join(PUBLIC, page)));
      return;
    }
    // The static site injects the deployed wss:// URL via config.js; locally we
    // serve it empty so the client falls back to ws://<this host>.
    if (req.url?.startsWith("/config.js")) {
      res.writeHead(200, { "content-type": "application/javascript; charset=utf-8" });
      res.end(`window.__WS_URL__ = "${process.env.WS_URL ?? ""}";\n`);
      return;
    }
    // Dev-only: what the store holds for a connection — to see that client.data
    // was persisted, and that it is gone after $disconnect.
    const inspect = req.method === "GET" ? /^\/__connections\/([^/?]+)/.exec(req.url ?? "") : null;
    if (inspect) {
      store
        .get(decodeURIComponent(inspect[1]))
        .then(meta => {
          res.writeHead(meta ? 200 : 404, { "content-type": "application/json" });
          res.end(JSON.stringify(meta ?? { error: "not found" }));
        })
        .catch(err => {
          res.writeHead(500, { "content-type": "text/plain" });
          res.end(String(err?.stack ?? err));
        });
      return;
    }
    // Dev-only: simulate a redeploy = a brand-new Lambda instance (keeps live
    // sockets + the durable store).
    if (req.method === "POST" && req.url?.startsWith("/__reload")) {
      reload()
        .then(instance => {
          res.writeHead(200, { "content-type": "application/json" });
          res.end(JSON.stringify({ ok: true, instance }));
        })
        .catch(err => {
          res.writeHead(500, { "content-type": "text/plain" });
          res.end(String(err?.stack ?? err));
        });
      return;
    }
    res.writeHead(404, { "content-type": "text/plain" });
    res.end("not found");
  });

  // The WebSocket endpoint on the same port, upgraded by hand so $connect can
  // refuse BEFORE the handshake completes. handleProtocols echoes whatever the
  // bridge negotiated — what API Gateway does with the $connect response's
  // Sec-WebSocket-Protocol.
  const wss = new WebSocketServer({
    noServer: true,
    handleProtocols: (_offered, req) => (req as any)[NEGOTIATED] ?? false,
  });
  const open = new Map<string, WebSocket>();

  server.on("upgrade", async (req, socket, head) => {
    const connectionId = randomUUID();
    const info: ConnectionInfo = {
      connectedAt: Date.now(),
      sourceIp: req.socket.remoteAddress,
      userAgent: req.headers["user-agent"],
    };
    let hungUp = false;
    socket.on("error", () => {}); // a client vanishing mid-$connect is not a crash
    socket.once("close", () => (hungUp = true));

    const url = new URL(req.url ?? "/", "http://localhost");
    const result = await dispatch(
      gwEvent(connectionId, "connect", info, port, {
        headers: req.headers as Record<string, string>,
        query: Object.fromEntries(url.searchParams),
      }),
    );
    if (result.statusCode < 200 || result.statusCode > 299) {
      refuseUpgrade(socket, result.statusCode, result.body);
      return;
    }
    if (hungUp || socket.destroyed) {
      // Accepted, but nobody is there any more: don't leave it in the store.
      await dispatch(gwEvent(connectionId, "disconnect", info, port));
      return;
    }
    (req as any)[NEGOTIATED] = result.headers?.["Sec-WebSocket-Protocol"];
    wss.handleUpgrade(req, socket, head, ws => accept(ws, connectionId, info));
  });

  function accept(socket: WebSocket, connectionId: string, info: ConnectionInfo) {
    LocalSocketRegistry.set(connectionId, socket); // so pushes (and kicks) reach it
    open.set(connectionId, socket);

    // Let the client know its id (handy in the UI; not part of API Gateway).
    // Skipped on a graphql-transport-ws socket: that protocol is strict about
    // unknown messages and the client would close the connection on it.
    if (socket.protocol !== GRAPHQL_TRANSPORT_WS_PROTOCOL) {
      socket.send(JSON.stringify({ event: "$connected", data: { connectionId } }));
    }

    socket.on("message", async raw => {
      // API Gateway with a $default route delivers the raw frame as body.
      await dispatch(gwEvent(connectionId, "message", info, port, { body: raw.toString() }));
    });

    socket.on("close", async (code, reason) => {
      open.delete(connectionId);
      await dispatch(
        gwEvent(connectionId, "disconnect", info, port, {
          disconnect: { code, reason: reason.toString() },
        }),
      );
      LocalSocketRegistry.delete(connectionId);
    });
  }

  // Build the first "instance" before accepting traffic.
  await reload();
  await new Promise<void>(resolve => server.listen(port, resolve));
  port = (server.address() as AddressInfo).port;

  return {
    port,
    wsUrl: `ws://localhost:${port}`,
    httpUrl: `http://localhost:${port}`,
    store,
    reload,
    httpPlatform: () => app!.getHttpAdapter().getType(),
    async close() {
      for (const socket of open.values()) socket.terminate();
      wss.close();
      await new Promise<void>(resolve => server.close(() => resolve()));
      await app?.close();
    },
  };
}
