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
 *  direct `express` dependency. In Lambda mode the server never listens, so the
 *  route is inert there (the Lambda handler calls bridge.dispatch directly).
 *
 *  NOTE: streams are Lambda-only by design — there is no HTTP /stream route.
 * ========================================================================== */

import { HttpServer, INestApplication, WebSocketAdapter } from "@nestjs/common";
import { EventEmitter } from "events";
import { Observable, isObservable } from "rxjs";
import { filter } from "rxjs/operators";
import { DISPATCH_PATH } from "./config";
import { ApiGwWsEvent, ClientFrame } from "./contract";
import { GatewayBridge, GatewayClient, BoundHandler } from "./gateway-bridge";
import { enqueueBroadcast } from "./broadcast-queue";

export interface ApiGatewayWsAdapterOptions {
  /** Path the API Gateway HTTP integration POSTs WebSocket events to. */
  dispatchPath?: string;
}

export class ApiGatewayWsAdapter implements WebSocketAdapter {
  httpAdapter: HttpServer;
  constructor(
    protected readonly app: INestApplication,
    private readonly bridge: GatewayBridge,
    options: ApiGatewayWsAdapterOptions = {},
  ) {
    this.httpAdapter = app.getHttpAdapter();
    this.registerDispatchRoute(options.dispatchPath ?? DISPATCH_PATH);
  }

  /* ---- inbound: API Gateway HTTP integration, registered on the adapter ---- */

  private registerDispatchRoute(path: string) {
    this.httpAdapter.post(path, async (req: any, res: any) => {
      let event: ApiGwWsEvent;
      try {
        event = (await this.readJsonBody(req)) as ApiGwWsEvent;
      } catch {
        res.status(400);
        res.send("invalid JSON body");
        return;
      }
      const result = await this.bridge.dispatch(event);
      res.status(result.statusCode);
      res.send(result.body ?? "");
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
    return this.bridge.server;
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
