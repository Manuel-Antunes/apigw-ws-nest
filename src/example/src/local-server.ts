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
  ApiGwWsEvent,
  EVENT_TYPE,
  ROUTE,
  createNestApp,
  createGatewayBridge,
  GatewayBridge,
} from "../..";
import { AppModule } from "./app.module";

/* ---------------------------------------------------------------------------
 *  "Lambda instance" lifecycle (dev-only).
 *
 *  In production each warm Lambda container is one instance; a code refresh /
 *  redeploy gives you a BRAND-NEW instance (empty in-process memory) while API
 *  Gateway keeps the existing WebSocket connections open and the durable state
 *  (connections + rooms) lives in DynamoDB.
 *
 *  Locally we emulate that exactly: buildInstance() throws away the old Nest app
 *  + GatewayBridge (so the gateway's in-memory state — any rxjs Subjects, the
 *  bridge's client map — is wiped) and builds fresh ones, WITHOUT touching the
 *  live browser sockets (the ws server below) or the process-singleton store +
 *  LocalSocketRegistry (which stand in for DynamoDB + API Gateway @connections).
 *  Hit POST /__reload to simulate a redeploy mid-session.
 * ------------------------------------------------------------------------- */
let bridge: GatewayBridge;
let app: INestApplication | undefined;
let instanceGen = 0;

async function buildInstance() {
  const previous = app;
  // createGatewayBridge() reuses the memoized store + publisher singletons, so
  // room membership and live-socket routing survive — only the in-process
  // gateway/bridge state is reborn. That's what makes this a faithful "new
  // Lambda instance" rather than a full restart.
  bridge = createGatewayBridge();
  const next = await createNestApp(AppModule, bridge);
  await next.init();
  app = next;
  instanceGen += 1;
  if (previous) await previous.close(); // tear the old instance down once the new one is live
  // eslint-disable-next-line no-console
  console.log(`[instance ${instanceGen}] ready — connections + rooms preserved`);
}

/** Feed an API Gateway event into the CURRENT instance's bridge. */
const dispatch = (event: ApiGwWsEvent) => bridge.dispatch(event);

const PORT = Number(process.env.PORT ?? 6005);
const INDEX = path.join(__dirname, "..", "public", "index.html");
const CHAT = path.join(__dirname, "..", "public", "chat.html");

type Phase = "connect" | "disconnect" | "message";

function gwEvent(
  connectionId: string,
  phase: Phase,
  body?: string,
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
  const wss = new WebSocketServer({ server });

  wss.on("connection", async socket => {
    const connectionId = randomUUID();
    LocalSocketRegistry.set(connectionId, socket); // so pushes can reach it

    // Let the client know its id (handy in the UI; not part of API Gateway).
    socket.send(
      JSON.stringify({ event: "$connected", data: { connectionId } }),
    );
    await dispatch(gwEvent(connectionId, "connect"));

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
    console.log(`  websocket   : ws://localhost:${PORT}`);
    console.log(`  reload      : POST http://localhost:${PORT}/__reload  (= new Lambda instance)`);
  });
}

main();
