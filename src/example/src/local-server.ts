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

import * as http from "http";
import * as fs from "fs";
import * as path from "path";
import { randomUUID } from "crypto";
import { WebSocketServer } from "ws";
import { handler } from "./handler";
import { LocalSocketRegistry, ApiGwWsEvent, EVENT_TYPE, ROUTE } from "../..";

const PORT = Number(process.env.PORT ?? 6005);
const INDEX = path.join(__dirname, "..", "public", "index.html");

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
    // The static site injects the deployed wss:// URL via config.js; locally we
    // serve it empty so the client falls back to ws://<this host>.
    if (req.url?.startsWith("/config.js")) {
      res.writeHead(200, {
        "content-type": "application/javascript; charset=utf-8",
      });
      res.end(
        "/* local: no injected WS_URL — client falls back to location.host */",
      );
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
    await handler(gwEvent(connectionId, "connect"));

    socket.on("message", async raw => {
      // API Gateway with a $default route delivers the raw frame as body.
      await handler(gwEvent(connectionId, "message", raw.toString()));
    });

    socket.on("close", async () => {
      await handler(gwEvent(connectionId, "disconnect"));
      LocalSocketRegistry.delete(connectionId);
    });
  });

  server.listen(PORT, () => {
    // eslint-disable-next-line no-console
    console.log(`local API Gateway emulator`);
    console.log(`  test client : http://localhost:${PORT}`);
    console.log(`  websocket   : ws://localhost:${PORT}`);
  });
}

main();
