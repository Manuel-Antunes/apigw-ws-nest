/* =============================================================================
 *  Local "API Gateway WebSocket" emulator  (dev only)
 * =============================================================================
 *  Runs the REAL Lambda handler against REAL browser WebSockets, so you can test
 *  the whole flow without any AWS. It plays the role API Gateway plays in prod:
 *
 *    browser ws  --connect-->  this server  --CONNECT  event-->  handler()
 *    browser msg --------->     this server  --MESSAGE  event-->  handler()
 *    handler push --------->    LocalPublisher --registry--> browser ws.send()
 *
 *  RT_PROVIDER is forced to 'local' (BEFORE any lib import) so the publisher
 *  routes through the in-memory socket registry instead of the AWS Management API.
 * ========================================================================== */

process.env.RT_PROVIDER = "local";
import "dotenv/config";
import * as http from "http";
import * as fs from "fs";
import * as path from "path";
import { randomUUID } from "crypto";
import { WebSocketServer } from "ws";
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

/* ---------------------------------------------------------------------------
 *  "Lambda instance" lifecycle (dev-only).
 *
 *  In production each warm Lambda container is one instance; a code refresh /
 *  redeploy gives you a BRAND-NEW instance (empty in-process memory) while API
 *  Gateway keeps the existing WebSocket connections open and the durable state
 *  (connections, rooms, subscriptions) lives in DynamoDB.
 *
 *  Locally we emulate that exactly: buildInstance() throws away the old Nest app
 *  AND the old GatewayBridge — so the bridge's client map, any rxjs Subjects and
 *  every protocol handler are reborn — while the things that stand in for
 *  DynamoDB are created ONCE, here, and handed to each new bridge.
 *
 *  Those two variables below are the whole point of this file. They used to be
 *  hidden memoized singletons inside the library, which meant the emulator was
 *  only accidentally faithful: nothing said out loud "these must outlive the
 *  app". Now the substitution is explicit, and if you delete either line the
 *  cross-instance test fails immediately — which is exactly the failure a
 *  redeploy would cause in production.
 *
 *  Hit POST /__reload to simulate a redeploy mid-session.
 * ------------------------------------------------------------------------- */
const store = new InMemoryConnectionStore();          // stands in for DynamoDB
const registry = new InMemorySubscriptionRegistry();  // ...and so does this

let bridge: GatewayBridge;
let app: INestApplication | undefined;
let instanceGen = 0;

async function buildInstance() {
  const previous = app;
  // A brand-new bridge (new client map, new flusher, new protocol handlers) over
  // the SAME durable store — a faithful "new Lambda instance" rather than a full
  // restart. LocalSocketRegistry, which stands in for API Gateway's @connections,
  // is untouched, so the live browser sockets survive too.
  bridge = GatewayBridge.builder().provider('local').store(store).build();
  const next = await createNestApp(AppModule, bridge);
  await next.init();
  // Re-registered against the NEW bridge, with the SAME registry and the same
  // PubSub — so subscriptions taken out before the reload still deliver after it.
  enableGraphQLSubscriptions(next, bridge, { registry, pubsub });
  app = next;
  instanceGen += 1;
  if (previous) await previous.close(); // tear the old instance down once the new one is live
  // eslint-disable-next-line no-console
  console.log(`[instance ${instanceGen}] ready — connections + subscriptions preserved`);
}

/** Feed an API Gateway event into the CURRENT instance's bridge. */
const dispatch = (event: ApiGwWsEvent) => bridge.dispatch(event);

const PORT = Number(process.env.PORT ?? 6005);
const INDEX = path.join(__dirname, "..", "public", "index.html");
const CHAT = path.join(__dirname, "..", "public", "chat.html");
const GRAPHQL = path.join(__dirname, "..", "public", "graphql.html");

type Phase = "connect" | "disconnect" | "message";

function gwEvent(
  connectionId: string,
  phase: Phase,
  body?: string,
  headers?: Record<string, string | undefined>,
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
      domainName: `localhost:${PORT}`,
      stage: "local",
      apiId: "local",
      requestId: randomUUID(),
      connectedAt: Date.now(),
      ...(phase === "message" ? { messageId: randomUUID() } : {}),
    },
    body,
    // API Gateway forwards the handshake headers on $connect only — that's where
    // Sec-WebSocket-Protocol comes from, so the bridge can negotiate it.
    ...(phase === "connect" ? { headers } : {}),
    isBase64Encoded: false,
  };
}

async function main() {
  // Plain HTTP server to serve the test client.
  const server = http.createServer((req, res) => {
    if (req.url === "/" || req.url?.startsWith("/index.html")) {
      res.writeHead(200, { "content-type": "text/html; charset=utf-8" });
      res.end(fs.readFileSync(INDEX));
      return;
    }
    if (req.url === "/chat" || req.url?.startsWith("/chat.html")) {
      res.writeHead(200, { "content-type": "text/html; charset=utf-8" });
      res.end(fs.readFileSync(CHAT));
      return;
    }
    if (req.url === "/graphql" || req.url?.startsWith("/graphql.html")) {
      res.writeHead(200, { "content-type": "text/html; charset=utf-8" });
      res.end(fs.readFileSync(GRAPHQL));
      return;
    }
    // The static site injects the deployed wss:// URL via config.js; locally we
    // serve it empty so the client falls back to ws://<this host>.
    if (req.url?.startsWith("/config.js")) {
      res.writeHead(200, {
        "content-type": "application/javascript; charset=utf-8",
      });
      res.end(`window.__WS_URL__ = "${process.env.WS_URL ?? ""}";\n`);
      return;
    }
    // Dev-only: simulate a code refresh / redeploy = a brand-new Lambda instance
    // (keeps live sockets + durable store). See buildInstance() above.
    if (req.method === "POST" && req.url?.startsWith("/__reload")) {
      buildInstance()
        .then(() => {
          res.writeHead(200, { "content-type": "application/json" });
          res.end(JSON.stringify({ ok: true, instance: instanceGen }));
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

  // Real WebSocket endpoint on the same port (ws://localhost:PORT).
  // handleProtocols mirrors what API Gateway does with the Sec-WebSocket-Protocol
  // header + the $connect response: browsers' graphql-ws client offers
  // 'graphql-transport-ws' and aborts the handshake unless the server echoes it.
  const wss = new WebSocketServer({
    server,
    handleProtocols: protocols =>
      protocols.has(GRAPHQL_TRANSPORT_WS_PROTOCOL) ? GRAPHQL_TRANSPORT_WS_PROTOCOL : false,
  });

  wss.on("connection", async (socket, req) => {
    const connectionId = randomUUID();
    LocalSocketRegistry.set(connectionId, socket); // so pushes can reach it

    // Let the client know its id (handy in the UI; not part of API Gateway).
    // Skipped on a graphql-transport-ws socket: that protocol is strict about
    // unknown messages and the client would close the connection on it.
    if (socket.protocol !== GRAPHQL_TRANSPORT_WS_PROTOCOL) {
      socket.send(JSON.stringify({ event: "$connected", data: { connectionId } }));
    }
    await dispatch(
      gwEvent(connectionId, "connect", undefined, req.headers as Record<string, string>),
    );

    socket.on("message", async raw => {
      // API Gateway with a $default route delivers the raw frame as body.
      await dispatch(gwEvent(connectionId, "message", raw.toString()));
    });

    socket.on("close", async () => {
      await dispatch(gwEvent(connectionId, "disconnect"));
      LocalSocketRegistry.delete(connectionId);
    });
  });

  // Build the first "instance" before accepting traffic.
  await buildInstance();

  server.listen(PORT, () => {
    // eslint-disable-next-line no-console
    console.log(`local API Gateway emulator`);
    console.log(`  test client : http://localhost:${PORT}`);
    console.log(`  multi-chat  : http://localhost:${PORT}/chat`);
    console.log(`  graphql     : http://localhost:${PORT}/graphql`);
    console.log(`  websocket   : ws://localhost:${PORT}`);
    console.log(`  reload      : POST http://localhost:${PORT}/__reload  (= new Lambda instance)`);
  });
}

main();
