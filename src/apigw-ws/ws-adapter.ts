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
import { MessageMappingProperties } from "@nestjs/websockets";
import { EventEmitter } from "events";
import { EMPTY, Observable } from "rxjs";
import { filter } from "rxjs/operators";
import { DISPATCH_PATH } from "./config";
import { ApiGwWsEvent, ClientFrame } from "./contract";
import { GatewayBridge, GatewayClient } from "./gateway-bridge";

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
    handlers: MessageMappingProperties[],
    process: (data: any) => Observable<any>,
  ) {
    // Install an AWAITABLE processor: resolves only after the handler's response
    // observable completes AND every emitted frame has been sent. dispatch()
    // awaits this so the Lambda stays warm until delivery finishes.
    client.handleFrame = (frame: ClientFrame) =>
      new Promise<void>(resolve => {
        const sends: Promise<void>[] = [];
        this.bindMessageHandler(frame, handlers, process)
          .pipe(filter(result => result != null))
          .subscribe({
            next: response =>
              sends.push(
                client
                  .send(response as ClientFrame)
                  .catch(e => this.bridge.onSendError(client, e)),
              ),
            error: e => {
              Promise.resolve(this.bridge.onSendError(client, e)).finally(() =>
                resolve(),
              );
            },
            complete: () => {
              Promise.all(sends).then(() => resolve());
            },
          });
      });
  }

  bindMessageHandler(
    frame: ClientFrame,
    handlers: MessageMappingProperties[],
    process: (data: any) => Observable<any>,
  ): Observable<any> {
    const handler = handlers.find(h => h.message === frame.event);
    if (!handler) return EMPTY;
    return process(handler.callback(frame.data));
  }

  close() {
    /* nothing to tear down — API Gateway owns the sockets */
  }
}
