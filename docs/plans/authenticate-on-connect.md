# Plan: authenticate (and reject) a connection at `$connect`

> **Status: plan only.** Nothing described here is implemented yet. Written against `main` at
> `d81bb64` (`apigw-ws-nest@2.0.0`). Every `file:line` below refers to that commit unless it names a
> NestJS package, in which case it refers to the published tarball of that exact version
> (`@nestjs/websockets@11.2.7`, `@nestjs/websockets@12.1.2`, `@nestjs/platform-ws@12.1.2`,
> `@nestjs/platform-socket.io@12.1.2`).
>
> Target versions: everything opt-in ships as **2.1.0** (semver minor). The default flips and the
> move of `@nestjs/*` to `peerDependencies` ship together as **3.0.0** (see
> [§11.4](#114-nestjs-as-peerdependencies-30--a-separate-major-item)).

## TL;DR

Today a connection cannot be refused, `handleConnection` never sees the handshake, and nothing
about *who* a connection is survives to the next frame on another Lambda container. The plan:

1. **A connect phase in the bridge.** `$connect` runs a chain of hooks before anything is written:
   framework-agnostic ones from the builder (`.onConnect(hook)`), then each protocol's
   `ProtocolHandler.onConnect`. A hook that throws refuses the socket with a status (401, 403, ...);
   nothing reaches the store.
2. **The Nest adapter routes `$connect` to `OnGatewayConnection.handleConnection(client)`**,
   awaited, with `client.handshake = { headers, query, subprotocols, subprotocol, sourceIp,
   connectedAt, ... }`. Throwing `UnauthorizedException` / `ForbiddenException` / `WsException`
   (or calling `client.disconnect()`, the Socket.IO idiom) rejects the connection.
   `handleDisconnect(client, reason)` is finally called too. Opt-in in 2.x
   (`lifecycle: 'connect'`), the default in 3.0.
3. **`client.data` (Socket.IO's convention) is persisted with the connection** in its `META` row and
   rehydrated once per (instance, connection) on later frames, so a plain Nest `CanActivate` guard
   reading `client.data` works on any instance. New port methods: `SessionMeta.data?`,
   `ConnectionStore.get?()`, `RealtimePublisher.disconnect?()`.
4. **`POST /@dispatch` can be turned off (`dispatchPath: false`) or protected (`dispatchSecret`).**
   Once identity is keyed by `connectionId`, that route is an authentication bypass.
5. **3.0.0:** `@nestjs/*`, `rxjs` and `reflect-metadata` become `peerDependencies` supporting Nest 11
   and 12; `@nestjs/platform-express` becomes an optional peer.

---

## Contents

1. [Problem](#1-problem)
2. [Goals and non-goals](#2-goals-and-non-goals)
3. [What "native" looks like in NestJS](#3-what-native-looks-like-in-nestjs)
4. [Design options considered](#4-design-options-considered)
5. [Recommended design](#5-recommended-design)
6. [Port and contract changes](#6-port-and-contract-changes)
7. [Lifecycle details](#7-lifecycle-details)
8. [GraphQL and `connection_init`](#8-graphql-and-connection_init)
9. [The HTTP dispatch route](#9-the-http-dispatch-route)
10. [Local emulator](#10-local-emulator)
11. [Compatibility, migration and versioning](#11-compatibility-migration-and-versioning)
12. [Testing plan](#12-testing-plan)
13. [Documentation changes](#13-documentation-changes)
14. [Task checklist](#14-task-checklist)
15. [Open questions and things to verify on AWS](#15-open-questions-and-things-to-verify-on-aws)

---

## 1. Problem

### 1.1 `$connect` cannot be refused

`GatewayBridge.dispatch` handles `CONNECT` in one fixed sequence (`src/gateway-bridge.ts:337-358`):
negotiate the subprotocol (`:339`), `store.add(connectionId, { connectedAt, subprotocol? })`
(`:340-343`), auto-join `GLOBAL_ROOM` when no subprotocol was negotiated (`:344-354`), and return
`200` (`:355-357`). There is no hook between receiving the handshake and accepting it.

The handshake itself is already modelled — `ApiGwWsEvent.headers` and `queryStringParameters`
(`src/contract.ts:58-60`) and `requestContext.identity.sourceIp`/`userAgent`
(`src/contract.ts:28-31, 50`) — but the bridge only ever reads one header, `Sec-WebSocket-Protocol`,
inside the private `negotiate()` (`src/gateway-bridge.ts:502-513`). `ProtocolHandler`
(`src/gateway-bridge.ts:186-211`) has `handleFrame` and `onDisconnect` but nothing for connect, so
not even a protocol plug-in can refuse a socket.

The contract already documents what a refusal would look like — "For `$connect`, a non-2xx rejects
the socket" (`src/contract.ts:64-66`) — the bridge just never produces one, except `500` when
something throws (`src/gateway-bridge.ts:378-382`).

### 1.2 `OnGatewayConnection.handleConnection` runs at the wrong time, with nothing to read

- The `'connection'` hub event is emitted only from `ensureClient` (`src/gateway-bridge.ts:436-444`),
  which is only called on the MESSAGE path (`:366`). So Nest calls `handleConnection` on the **first
  frame each Lambda container sees for a connection** — possibly several times per connection,
  never at `$connect`, and never for a connection that sends no frame.
- The `GatewayClient` it receives (`src/gateway-bridge.ts:71-116`) carries `connectionId`, the
  handler map and live subscriptions. No handshake, no data.
- Even if it had both, Nest would not let it refuse anything: it calls `handleConnection`
  fire-and-forget. `@nestjs/websockets@11.2.7` `web-sockets-controller.js:74-80` subscribes
  `(args) => instance.handleConnection(...args)` to an rxjs `Subject` and drops the returned
  promise; `@nestjs/websockets@12.1.2` does the same for static gateways
  (`web-sockets-controller.js:94-98` binds `instance.handleConnection?.bind(instance)`, `:127-136`
  subscribes it). An `async handleConnection` that throws is an unhandled rejection, not a refusal.
- `OnGatewayDisconnect.handleDisconnect` is never called at all: Nest only wires it through
  `adapter.bindClientDisconnect` when the adapter defines one (`@nestjs/websockets@11.2.7`
  `web-sockets-controller.js:64-66`, `@12.1.2` `:117-119`), and `ApiGatewayWsAdapter`
  (`src/ws-adapter.ts:66-197`) does not.

### 1.3 No identity survives across frames or instances

`SessionMeta` is `{ userId?, connectedAt, subprotocol? }` (`src/ports.ts:16-23`), and nothing in
the library ever writes `userId`. `ConnectionStore` (`src/ports.ts:37-47`) can add, remove, join,
leave and list room members, but cannot **read a connection back**. `DynamoConnectionStore.add`
writes the `META` row (`src/providers/aws.ts:26-33`) and nothing reads it;
`InMemoryConnectionStore` keeps `conns` (`src/providers/local.ts:17-27`) and nothing reads that
either. A guard on a `@SubscribeMessage` handler, running in a later invocation on another
container, has `client.connectionId` and nothing else.

### 1.4 `POST /@dispatch` is always registered, unauthenticated

`ApiGatewayWsAdapter`'s constructor always registers the raw dispatch route on the app's HTTP
server (`src/ws-adapter.ts:68-76`, path from `DISPATCH_PATH`, `src/config.ts:16`). The route parses
any JSON body as an API Gateway event and hands it to `bridge.dispatch` (`src/ws-adapter.ts:80-94`).
The comment says it is inert in Lambda mode because the server never listens
(`src/ws-adapter.ts:16-17`) — true for the `bridge.serve()` handler in this repository, false for
any app that *also* serves its Nest HTTP app from Lambda (a function URL, a Fastify streaming
handler, Lambda Web Adapter). There it is reachable from the internet.

Today that is a spoofing nuisance. **Once identity is keyed by `connectionId`, it is an
authentication bypass**: a forged `MESSAGE` event naming a live `connectionId` would be served with
that connection's rehydrated identity. So this feature cannot ship without a way to switch the route
off or authenticate it.

Related gap in the same function: it copies `statusCode` and `body` but drops `result.headers`
(`src/ws-adapter.ts:90-92`), so in HTTP mode the `Sec-WebSocket-Protocol` echo
(`src/gateway-bridge.ts:355-356`) never leaves the process.

### 1.5 `@nestjs/*` are hard dependencies pinned to `^11`

`package.json:78-85` lists `@nestjs/common`, `@nestjs/core`, `@nestjs/platform-express` and
`@nestjs/websockets` at `^11.0.0` (plus `reflect-metadata`, `rxjs`) as `dependencies`. A Nest 12 app
therefore installs a second framework next to its own, unless it forces a single copy with
package-manager overrides. The README already suggests the fix in its caveats
(`README.md:1222-1223`). Three facts make it more pressing for Nest 12:

- Nest 12 packages are **ESM-only** (`"type": "module"` in `@nestjs/websockets@12.1.2`
  `package.json`) and declare `engines.node >= 20` — so does Nest 11 (`npm view @nestjs/core@11
  engines`), while this package still says `>=18` (`package.json:41-43`).
- `@nestjs/platform-express` is a hard dependency even for Fastify apps; it is only needed because
  `createNestApp` calls `NestFactory.create` without an adapter (`src/app-factory.ts:39-42`).
- The optional peer `@nestjs/graphql: ^12.0.0 || ^13.0.0` (`package.json:93`) excludes
  `@nestjs/graphql@14`, the line that peers on Nest 12.

### 1.6 The motivating example

A NestJS 12 + Fastify service implements CopilotKit's "Intelligence" realtime gateway on top of this
library, speaking Phoenix V2 frames through a custom `ProtocolHandler`. To authenticate it had to
build, around the library rather than with it:

| What it built | What it compensates for | What replaces it in this plan |
|---|---|---|
| `RealtimeBridge extends GatewayBridge`, overriding `dispatch` to call an injected authenticator on `CONNECT` and return `{ statusCode: 401 }` when it refuses | no connect hook (`src/gateway-bridge.ts:337-358`) | `handleConnection` that throws, or a builder/protocol `onConnect` hook |
| A per-connection "grant" persisted in its own Postgres table keyed by `connectionId` | `SessionMeta` has nowhere to put it and the store cannot read a connection back (§1.3) | `client.data`, persisted in the connection's `META` row |
| A `ProtocolHandler.onDisconnect` deleting the grant | the grant lives outside the store | `store.remove` already deletes `META`, and `data` with it |
| A Nest `CanActivate` guard loading the grant by `client.connectionId` on every frame | the client carries no identity | the same guard reads `client.data`, rehydrated once per instance |
| The provider presets rebuilt by hand (`InMemoryConnectionStore`/`LocalPublisher`/`InlineMessageBus`/`InMemoryDeliveryLedger` vs `DynamoConnectionStore`/`ApiGatewayPublisher`/`DynamoOutboxBus`/`DynamoDeliveryLedger`) | `build()` hardcodes `new GatewayBridge(...)` (`src/gateway-bridge.ts:638`) and `PRESETS` is module-private (`:518-540`) | no subclass needed; `build(ctor?)` for whoever still wants one |
| Package-manager overrides pinning one `@nestjs/*` | hard `^11` dependencies (§1.5) | `peerDependencies` in 3.0 |

Everything in that table is generic — none of it is specific to Phoenix or CopilotKit — which is why
it belongs in the library.

### 1.7 Related findings in the same code

Not the goal of this plan, but each one is touched by it, so they are listed with a disposition:

- **The emulator accepts before it asks.** `wss.on("connection")` runs after the upgrade has
  completed, dispatches `CONNECT` and ignores the result (`src/example/src/local-server.ts:191-203`);
  `handleProtocols` hardcodes the GraphQL subprotocol instead of echoing what the bridge negotiated
  (`:185-189`); `gwEvent` forwards no query string and no source IP (`:98-134`). **Fixed by §10.**
- **Sends the handler does not await can be lost on Lambda.** `GatewayClient.send/emit/sendRaw`
  (`src/gateway-bridge.ts:90-108`) return a promise but do not enqueue it in the dispatch scope.
  Nest's exception path does not await it: `WsProxy` calls the exception handler without awaiting
  (`@nestjs/websockets@12.1.2` `context/ws-proxy.js:17-26`), and `BaseWsExceptionFilter` answers
  with `client.emit('exception', ...)` (`exceptions/base-ws-exception-filter.js:26-43`). A guard's
  refusal — the visible end of this feature — can therefore be frozen mid-send. **Fixed by task 6.**
- **Room and GraphQL subscription rows carry no `ttl`** (`src/providers/aws.ts:75-82`,
  `src/graphql/subscription-registry.ts:108-130`); only `META` does (`src/providers/aws.ts:28-31`).
  A missed `$disconnect` leaves them forever. Per-user rooms joined at connect (§5.4) make that more
  common. **Follow-up, not in this plan.**
- **The per-instance client map is never evicted** on an instance that does not receive the
  connection's `$disconnect` (`src/gateway-bridge.ts:237, 457-470`). Harmless on Lambda, a slow leak
  on a long-lived HTTP instance. **Follow-up.**
- **README drift:** the Ports snippet (`README.md:1116-1131`) shows `toRoom` as required and omits
  `pageMembersOf`. **Fixed with the docs (§13).**

---

## 2. Goals and non-goals

**Goals**

1. Reject a connection at `$connect` with a chosen status (401, 403, 429, ...), writing nothing.
2. Establish identity **once**, at `$connect`, and make it available on every later frame on
   **any** instance — through standard Nest APIs: `@ConnectedSocket()`, `CanActivate` guards,
   `createParamDecorator`, `OnGatewayConnection`, `OnGatewayDisconnect`, `WsException`,
   `UnauthorizedException`.
3. A framework-agnostic path for bridges without Nest, and a protocol-level path for plug-ins that
   authenticate in their own terms (a bearer smuggled in `Sec-WebSocket-Protocol`, a Phoenix
   `join_token`).
4. Identical behaviour in **Lambda**, **HTTP/ECS** and the **local emulator**, including after a
   simulated redeploy (`POST /__reload`).
5. **Zero behaviour change for apps that do not opt in** — 2.1.0 is a semver minor.

**Non-goals**

- Changing identity after `$connect` (re-authentication, token refresh on an open socket,
  `connection_init`-based auth). Designed in [§8](#8-graphql-and-connection_init) as Phase 2, not
  built here.
- Running Nest guards, interceptors or pipes around `handleConnection`. Nest does not either (§3),
  and a class-level guard reading `client.data` would refuse every connection before it had any.
- Request-scoped gateways' `handleConnection` under `lifecycle: 'connect'` (refused at boot with a
  clear error, §5.5).
- Listing a user's connections (use a per-user room, §5.4), and fixing `ttl` on room/subscription
  rows (§1.7).
- An API Gateway Lambda REQUEST authorizer integration — kept compatible (the contract gains
  `requestContext.authorizer`), not built (§4, option D).

---

## 3. What "native" looks like in NestJS

| | Socket.IO (`IoAdapter`) | ws (`WsAdapter`) | This library, proposed |
|---|---|---|---|
| Handshake in `handleConnection` | `socket.handshake` (`headers`, `query`, `auth`, `address`, `time`, ...) | the upgrade `IncomingMessage` as a 2nd argument: `wsServer.emit('connection', ws, request)` (`@nestjs/platform-ws@12.1.2` `adapters/ws-adapter.js:200-202`) | `client.handshake` |
| Per-socket state | `socket.data` — restored by connection-state recovery, visible through `fetchSockets()` across a cluster | none | `client.data`, persisted and rehydrated |
| Rejecting | `io.use((socket, next) => next(new Error()))` middleware, or `client.disconnect()` inside `handleConnection` | `verifyClient`, or close the socket | throw from `handleConnection`, or `client.disconnect()` |
| Is `handleConnection` awaited? | no (fire-and-forget, §1.2) | no | **yes**, by the adapter (§5.5) |
| Guards / filters | around `@SubscribeMessage` only; a `false` guard throws `WsException('Forbidden resource')` (`@nestjs/websockets@12.1.2` `context/ws-context-creator.js:68`), which the base filter turns into an `exception` event | same | same — unchanged |

Nest 12 adds request-scoped gateways, whose `handleConnection` goes through
`createRequestScopedEventHandler` and the exception filters
(`@nestjs/websockets@12.1.2` `web-sockets-controller.js:216-250`), but even there a throw is caught
and reported, not turned into a refusal.

So Nest has **no** native notion of "reject the handshake from `handleConnection`". The closest
idioms are Socket.IO's `client.disconnect()` in `handleConnection` and throwing a Nest exception.
The design supports both, and maps the exception's status onto the `$connect` response.

---

## 4. Design options considered

**A. A builder-level hook: `.authenticate(handshake => identity | null)`.** Simple, framework-agnostic,
works for any protocol. But it lives outside Nest's DI unless the caller closes over
`app.get(AuthService)`, it is invisible to someone reading the gateways, and on its own it does
nothing for `OnGatewayConnection`, which keeps running at the wrong time. Generalised below into
`.onConnect((client, event) => ...)`, which subsumes it (the client carries both the handshake and
`data`).

**B. Route `$connect` to the gateways' `handleConnection(client)`, awaited, with a handshake-bearing
`GatewayClient`; throwing rejects.** The most Nest-native: DI, `@Inject`, Nest exceptions, the same
method a Socket.IO gateway already has. Costs: Nest calls `handleConnection` fire-and-forget (§1.2),
so the adapter has to take that call over (§5.5); request-scoped gateways need separate handling.

**C. `ProtocolHandler.onConnect(client, event)`.** The right place for auth that belongs to a wire
protocol rather than to the app — a bearer offered as a `Sec-WebSocket-Protocol` entry, a Phoenix
`join_token`. Same shape as `onDisconnect` (`src/gateway-bridge.ts:208-210`), so plug-ins keep one
registration point. Not enough alone for Nest users, who think in gateways.

**D. An API Gateway Lambda REQUEST authorizer on `$connect`.** AWS refuses the socket before the app
runs, the result is cacheable, and API Gateway repeats the authorizer's `context` on every later
route of that connection, so identity needs no storage. But it is a second Lambda configuration,
AWS-only (the emulator would have to imitate it), its `context` values are flat strings, numbers or
booleans, and its code lives outside the Nest app. Left **compatible** (the contract gains
`requestContext.authorizer`, surfaced as `client.handshake.authorizer`); a future
"`bridge.serve()` answers authorizer events by running the same connect hooks" is noted in §15.

**E. Status quo: subclass `GatewayBridge` and keep identity in your own table** (§1.6). Works, and is
exactly the boilerplate this plan removes; it also leaves the `/@dispatch` hole and the dropped
`handleDisconnect`.

| | A builder hook | B `handleConnection` | C protocol hook | D API GW authorizer | E subclass |
|---|---|---|---|---|---|
| Feels like Nest | partly | **yes** | no | no | no |
| Works without Nest | **yes** | no | **yes** | **yes** | yes |
| Protocol-specific auth | via `event` | via `client.handshake` | **yes** | headers/query only | yes |
| Identity on later frames, any instance | needs storage | needs storage | needs storage | **built in** | own table |
| Same in Lambda / HTTP / emulator | **yes** | **yes** | **yes** | Lambda only | yes |
| Implementation risk | low | medium (§5.5) | low | medium | none (for the library) |

**Recommendation:** B + C on top of A, sharing one mechanism — a connect phase in the bridge, with
`client.data` as the single place identity lives — so whichever layer authenticates, a guard reads
the same thing. D stays compatible. E becomes unnecessary.

---

## 5. Recommended design

### 5.1 Shape

```
$connect ──► GatewayBridge (connect phase)
              1. handshake + subprotocol negotiation     (pure, nothing written)
              2. builder hooks       .onConnect(hook)    (framework-agnostic)
              3. protocol hooks      ProtocolHandler.onConnect
                   └─ NestGatewayProtocol.onConnect ──► each gateway's handleConnection(client)
              4. accept: store.add(META + data) → join GLOBAL_ROOM? → recorded joins
                 or reject: { statusCode }, nothing written

MESSAGE  ──► materialize client (cached, or rebuilt from store.get → client.data)
              └─ Nest binds handlers → guards read client.data → @SubscribeMessage

$disconnect / 410 ──► protocols' onDisconnect(id, client, reason)
                         └─ NestGatewayProtocol ──► handleDisconnect(client, reason)
                      → store.remove (META and data gone)
```

### 5.2 API sketch

Nest app — the whole feature from a gateway's point of view:

```ts
import { Inject, Injectable, CanActivate, ExecutionContext, UnauthorizedException, UseGuards } from '@nestjs/common';
import { WebSocketGateway, OnGatewayConnection, OnGatewayDisconnect, SubscribeMessage, ConnectedSocket } from '@nestjs/websockets';
import { GatewayClient } from 'apigw-ws-nest';

type Identity = { user: { id: string; roles: string[] } };

@WebSocketGateway()
export class RealtimeGateway implements OnGatewayConnection, OnGatewayDisconnect {
  constructor(@Inject(AuthService) private readonly auth: AuthService) {}

  // Runs ONCE per connection, at $connect, awaited. Throwing rejects the socket.
  async handleConnection(client: GatewayClient<Identity>) {
    const user = await this.auth.verify(client.handshake.query.token);
    if (!user) throw new UnauthorizedException();          // -> 401 at $connect
    client.data.user = { id: user.id, roles: user.roles };  // persisted with the connection
    await client.join(`user:${user.id}`);                   // applied once accepted (§7.5)
  }

  handleDisconnect(client: GatewayClient<Identity>, reason: string) {
    // client.data is still readable here; the META row is removed right after.
  }

  @UseGuards(WsUserGuard)
  @SubscribeMessage('whoami')
  whoami(@ConnectedSocket() client: GatewayClient<Identity>) {
    return { event: 'whoami', data: client.data.user };
  }
}

@Injectable()
export class WsUserGuard implements CanActivate {
  canActivate(context: ExecutionContext) {
    // On ANY instance: the bridge rehydrated client.data from the store before Nest got the frame.
    return !!context.switchToWs().getClient<GatewayClient<Partial<Identity>>>().data.user;
  }
}

// bootstrap — the only new line is the options object
app.useWebSocketAdapter(new ApiGatewayWsAdapter(app, bridge, {
  lifecycle: 'connect',   // route $connect/$disconnect to handleConnection/handleDisconnect (default in 3.0)
  dispatchPath: false,    // Lambda: no HTTP dispatch route at all (§9)
}));
```

Framework-agnostic, and protocol-level:

```ts
const bridge = GatewayBridge.builder()
  .provider('aws')
  .onConnect(async (client, event) => {               // repeatable, runs in registration order
    if (await rateLimited(client.handshake.sourceIp)) throw new ConnectionRejectedError(429);
  })
  .use(new PhoenixProtocol())                         // may implement onConnect(client, event)
  .connectTimeout(10_000)                             // default 10s; on expiry -> 503
  .maxConnectionData(16 * 1024)                       // default 16 KiB of JSON
  .build();                                           // or .build(MyBridge) for a subclass

bridge.onConnect(hook);                               // same as the builder method, after build()
await bridge.disconnect(connectionId);                // server-side kick (DeleteConnection)
```

New and changed exports:

```ts
// src/gateway-bridge.ts
export interface Handshake {
  /** Header names lower-cased (as Node and Socket.IO do). Connect phase only — see §5.4. */
  headers: Record<string, string>;
  /** queryStringParameters, undefined values dropped. Connect phase only. */
  query: Record<string, string>;
  /** Every subprotocol the client offered, in order. A bearer smuggled as an entry is readable
   *  here and never echoed (negotiation only picks registered ones). Connect phase only. */
  subprotocols: string[];
  /** The subprotocol the bridge accepted and echoed, if any. Persisted. */
  subprotocol?: string;
  /** requestContext.identity.sourceIp. Persisted. */
  sourceIp?: string;
  /** requestContext.identity.userAgent. Connect phase only. */
  userAgent?: string;
  /** requestContext.connectedAt, else Date.now(). Persisted. */
  connectedAt: number;
  /** requestContext.authorizer, when an API Gateway Lambda authorizer ran. */
  authorizer?: Record<string, unknown>;
}

export type ConnectHook = (client: GatewayClient, event: ApiGwWsEvent) => void | Promise<void>;

export class GatewayClient<TData extends object = Record<string, any>> {
  handshake: Handshake;
  /** Socket.IO's socket.data. Writable during the connect phase; persisted on accept; frozen
   *  (deep) afterwards on every instance. `{}` when nothing was set. */
  data: TData;
  /** true when this object was rebuilt from the store rather than created by $connect here. */
  readonly rehydrated: boolean;
  /** Socket.IO's socket.disconnect(true). During the connect phase: reject with 403. */
  disconnect(): Promise<void>;
  // unchanged: connectionId, handlers, subscriptions, handleFrame, send, sendRaw, emit, join, leave
}

export interface ProtocolHandler {
  // ...existing members...
  /** $connect, before anything is persisted. Throw (or client.disconnect()) to reject. */
  onConnect?(client: GatewayClient, event: ApiGwWsEvent): void | Promise<void>;
  /** Existing hook; two optional parameters added (backward compatible). */
  onDisconnect?(connectionId: string, client?: GatewayClient, reason?: string): void | Promise<void>;
}

// src/ports.ts — framework-agnostic rejection, identified by name like ConnectionGoneError
export class ConnectionRejectedError extends Error {
  readonly name = 'ConnectionRejectedError';
  constructor(readonly statusCode = 401, message = 'connection rejected') { super(message); }
}
export const isConnectionRejected = (err: unknown): boolean =>
  !!err && (err as any).name === 'ConnectionRejectedError';

// src/ws-adapter.ts
export interface ApiGatewayWsAdapterOptions {
  /** false = register no HTTP route. 2.x default '/@dispatch' (APIGW_DISPATCH_PATH); 3.0: unset = false. */
  dispatchPath?: string | false;
  /** Required value of `dispatchSecretHeader` on the dispatch route. Default APIGW_DISPATCH_SECRET. */
  dispatchSecret?: string;
  /** Default 'x-apigw-dispatch-secret'. */
  dispatchSecretHeader?: string;
  /** 'legacy' (2.x default): today's behaviour. 'connect' (3.0 default): §5.5. */
  lifecycle?: 'legacy' | 'connect';
}
```

`GatewayClient`'s constructor stays `(connectionId, store, publisher)` and gains an optional fourth
argument (`{ handshake?, data?, rehydrated?, phase? }`), so code that constructs one keeps
compiling.

### 5.3 How a rejection becomes a status

The bridge resolves the status **by shape, never by `instanceof`** — the same reason
`isConnectionGone` exists (`src/ports.ts:148-160`: duplicated bundle copies), and it keeps the core
free of a Nest import:

| What the hook did | `$connect` answers |
|---|---|
| threw `ConnectionRejectedError(status)` | `status` |
| called `client.disconnect()` | `403` |
| threw an object with `getStatus()` — any Nest `HttpException` (`UnauthorizedException` 401, `ForbiddenException` 403, `new HttpException(msg, 429)`) | `getStatus()` when it is 4xx, else `500` |
| threw an object with `getError()` (a `WsException`) whose error is an object with a numeric `status` or `statusCode` in 4xx — `new WsException({ status: 403, message })` | that status |
| threw any other `WsException` | `401` |
| exceeded `connectTimeout` | `503` |
| threw anything else (database down, a bug) | `500` — fail closed |

Body: the exception's message for 4xx (truncated to 200 characters), `"connect failed"` for 5xx.
4xx are expected traffic and are not logged by default; 5xx are logged as
`[connect: <hook or gateway name>]` like the existing `[disconnect: ...]` lines
(`src/gateway-bridge.ts:463`). No rejection writes anything to the store, and no `$disconnect`
follows one.

### 5.4 The contracts of `client.handshake` and `client.data`

**One rule decides both: what a client looks like after `$connect` is the same on every
instance.** The library's whole design argues against "works on the warm container, breaks on a cold
one" (`README.md:783-800`), so:

- **`client.handshake` is complete only during the connect phase.** On accept it is reduced, on the
  connecting instance too, to what is persisted: `connectedAt`, `subprotocol`, `sourceIp`. `headers`,
  `query` and `subprotocols` become empty. A guard reading `client.handshake.headers.authorization`
  therefore fails immediately in development, instead of passing on the warm instance and failing on
  scale-out. Raw headers are deliberately not persisted: they are the bearer tokens.
- **`client.data` is normalised on accept with a JSON round trip**, on the connecting instance too, so
  the object a guard reads is the same shape everywhere (a `Date` becomes an ISO string on *every*
  instance, not only the cold ones). Its UTF-8 JSON size must not exceed `maxConnectionData`
  (default 16 KiB, a ceiling rather than a target: `META` is written at 1 write unit per KB) — over
  the limit is a programming error and answers `500`.
- **`client.data` is frozen (deep) after accept and after rehydration.** Writing to it in a message
  handler throws a `TypeError` instead of silently creating per-instance state, which is the
  cross-instance rule enforced. Changing identity after connect is Phase 2 (§8).
- **Do not put secrets in it.** It is stored in DynamoDB in clear: keep ids and claims, not raw
  tokens.
- **Identity does not expire with the token.** API Gateway keeps a socket up to two hours; put an
  `expiresAt` in `data` and check it in the guard, and use `client.disconnect()` /
  `bridge.disconnect(id)` to kick.
- **Per-user rooms are the index.** `client.join(\`user:${id}\`)` in `handleConnection` makes
  `server.to('user:42').emit(...)` reach every socket of a user and `store.membersOf('user:42')` list
  them, with no new port.
- **In `lifecycle: 'legacy'` with no hooks,** `client.handshake` is the minimal persisted subset and
  `client.data` is `{}`, unpersisted and not frozen — today's semantics, unchanged.

### 5.5 How the adapter routes `$connect` to `handleConnection`

The difficulty is entirely on Nest's side: Nest calls `handleConnection` when the hub emits
`'connection'`, synchronously, and discards the promise (§1.2). And the adapter cannot stop emitting
`'connection'`: the same Nest callback also binds the gateway's message handlers to the client
(`@nestjs/websockets@12.1.2` `web-sockets-controller.js:111-121`; `@11.2.7` `:60-67`), and the
bridge needs that on every instance that materialises the client. So with `lifecycle: 'connect'` the
adapter **takes the two lifecycle hooks over**:

1. **Discover the gateways the way Nest does.** Nest's `SocketModule` walks every module's providers
   and keeps those whose metatype carries `GATEWAY_METADATA` = `'websockets:is_gateway'`
   (`@nestjs/websockets@12.1.2` `socket-module.js:36-47`, `constants.js:5`; the same key in 11,
   `constants.js:8`). The adapter does the same with
   `new DiscoveryService(app.get(ModulesContainer)).getProviders()` (both exported from
   `@nestjs/core` in 11 and 12).
2. **When:** on the first `create()` call. Nest calls `adapter.create()` from
   `SocketServerProvider.createSocketServer` during `app.init()` (`@12.1.2`
   `socket-server-provider.js:22-31`), *before* it binds that gateway's hooks
   (`web-sockets-controller.js:92-98`), and later gateways reuse the cached server. Instances already
   exist by then (`NestFactory.create` instantiates providers). The constructor would also work, since
   the adapter must be installed before `init()` anyway (`src/app-factory.ts:14-18`).
3. **For each gateway with `handleConnection`:** keep the original method for the adapter's own use,
   and install an own-property shim on the instance that **does nothing when Nest calls it for a
   client the bridge is materialising** (the bridge marks the client while it emits
   `'connection'`), and forwards otherwise. Nest 12 binds `instance.handleConnection` at
   subscription time (`web-sockets-controller.js:95`), which happens after `create()`, so it binds the
   shim; Nest 11 looks the property up at call time (`:78`), so it calls the shim. Either way Nest's
   fire-and-forget call becomes a no-op and the adapter's awaited call at `$connect` is the only
   real one.
4. **`handleDisconnect` needs no shim:** Nest only calls it through `bindClientDisconnect`, which this
   adapter keeps not implementing. The adapter calls it itself.
5. **`NestGatewayProtocol` gains `onConnect(client)`:** await each recorded `handleConnection(client)`
   in discovery order (module registration order, then provider order — the order Nest binds them),
   fail-fast on the first throw. And `onDisconnect(id, client, reason)`: call each
   `handleDisconnect(client, reason)`, logging and swallowing errors (a disconnect cannot be
   refused). Only the client is passed — Socket.IO's signature, not ws's `(client, request)`.
   Both exist **only under `lifecycle: 'connect'`**: in `'legacy'` the protocol has no `onConnect`,
   so the bridge stays on its unchanged fast path (§7.1).
6. **Request-scoped gateways** (Nest 12, `isDependencyTreeStatic()` false) go through
   `createRequestScopedEventHandler` (`web-sockets-controller.js:216-250`), which instantiates a fresh
   gateway per context and calls *its* `handleConnection`, so the shim cannot reach it. Under
   `lifecycle: 'connect'` the adapter throws at boot when a request-scoped gateway implements either
   hook, naming the class. Documented as a non-goal.
7. **Which connections:** every connection, whatever subprotocol it negotiated — Nest's semantic is
   "the gateway server's hub saw a client", and today `ensureClient` already emits `'connection'` for
   GraphQL sockets too (`src/gateway-bridge.ts:366`, before protocol routing). A gateway that only
   cares about `{ event, data }` sockets checks `client.handshake.subprotocol`.

Why a shim rather than wrapping prototypes or re-implementing binding: an own property on the one
static instance touches no other object, survives Nest 11 and 12's two binding styles, and leaves the
method callable by user code with its normal behaviour.

### 5.6 `handleDisconnect`, finally

`OnGatewayDisconnect.handleDisconnect(client, reason)` is called from the bridge's cleanup, before
the store row is removed, for every connection that had been accepted (§7.3), with `client.data`
readable. `reason` is `requestContext.disconnectReason` on `$disconnect`, `'gone'` after a `410`,
`'server disconnect'` after `bridge.disconnect()`. With `lifecycle: 'legacy'` it stays uncalled, as
today.

---

## 6. Port and contract changes

All additive in 2.1.0; every new port member is optional.

**`SessionMeta`** (`src/ports.ts:16-23`):

```ts
export interface SessionMeta {
  userId?: string;                  // unchanged; still never written by the library — prefer data
  connectedAt: number;
  subprotocol?: string;
  /** The connection's client.data, JSON-normalised, ≤ maxConnectionData bytes. */
  data?: Record<string, unknown>;
  /** requestContext.identity.sourceIp at $connect. */
  sourceIp?: string;
}
```

**`ConnectionStore`** (`src/ports.ts:37-47`):

```ts
/** Read a connection back. Optional in 2.x so custom stores keep compiling; required in 3.0.
 *  MUST be strongly consistent with add(): a frame can reach another instance right after
 *  $connect answered. MUST return null for an unknown or expired connection. */
get?(connectionId: string): Promise<SessionMeta | null>;
```

Registering a connect hook (builder, `bridge.onConnect`, a protocol with `onConnect`) on a bridge
whose store has no `get` throws at registration: "connect hooks need `ConnectionStore.get()` to
rehydrate `client.data` on other instances". Failing at boot beats a guard that passes on one
container and refuses on the next.

**`RealtimePublisher`** (`src/ports.ts:56-64`):

```ts
/** Close a connection server-side. aws: DeleteConnection; local: close the emulator's socket.
 *  Resolves (does not throw) when the connection is already gone. */
disconnect?(connectionId: string): Promise<void>;
```

**`DynamoConnectionStore`** (`src/providers/aws.ts`):

- `add()` (`:26-33`) writes `data` (a native map) and `sourceIp` only when present — this client is
  `docClient()` without `removeUndefinedValues` (`:22-24`), so an `undefined` attribute would throw.
  The JSON normalisation guarantees `data` itself has none.
- `get()`: `GetCommand({ Key: { pk: 'CONN#<id>', sk: 'META' }, ConsistentRead: true })`, strip
  `pk`/`sk`/`ttl`, and return `null` when the item is missing **or its `ttl` is in the past** —
  DynamoDB deletes expired items lazily, often days later, and an expired row must not authenticate
  anybody. `ConsistentRead` for the same reason `membersQuery` uses it (`:100-104`).
- `data` is a DynamoDB reserved word: any future expression on it (Phase 2's `update`) needs
  `ExpressionAttributeNames: { '#data': 'data' }`.

**`InMemoryConnectionStore`** (`src/providers/local.ts:17-42`): `get()` returns
`structuredClone(meta)` (or `null`), so mutating what it returns cannot leak back into the "table" —
the same isolation DynamoDB gives.

**`ApiGatewayPublisher`** (`src/providers/aws.ts:122-189`): `disconnect()` sends
`DeleteConnectionCommand`, treating `GoneException` as success. The `link: [api]` grant already
covers it (`execute-api:ManageConnections`, `sst.config.ts:155-159`). **`LocalPublisher`**: close the
registry's socket; `LocalSocketRegistry` entries (`src/providers/local.ts:15`) gain an optional
`close()`.

**Contract** (`src/contract.ts`):

```ts
export interface ApiGwRequestContext {
  // ...
  /** $disconnect only. */
  disconnectStatusCode?: number;
  disconnectReason?: string;
  /** Present when a Lambda REQUEST authorizer guards $connect; repeated on later routes. */
  authorizer?: Record<string, unknown>;
}
export interface ApiGwWsEvent {
  // ...
  multiValueHeaders?: Record<string, string[] | undefined>;
  multiValueQueryStringParameters?: Record<string, string[] | undefined>;
}
```

**DynamoDB data model** — the `META` row (`README.md:984`) becomes:

| `pk` | `sk` | attributes |
|---|---|---|
| `CONN#<id>` | `META` | `connectedAt`, `subprotocol?`, `sourceIp?`, `data?` (map), `ttl` |

**TTL interplay.** `META.ttl` is connect time + 3 h (`src/providers/aws.ts:28-29`), longer than API
Gateway's 2 h maximum connection duration, so `data` lives exactly as long as the connection could,
and expires with `META` when `$disconnect` never arrives.

**`$disconnect` and `410 Gone`.** Both already end in `bridge.cleanup` (`src/gateway-bridge.ts:360,
448, 253` and the GraphQL handler's `src/graphql/handler.ts:401-408`), which calls `store.remove`,
which deletes `META` (`src/providers/aws.ts:34-69`) — and `data` with it. No extra cleanup is needed,
which is what retires the downstream app's grant table.

---

## 7. Lifecycle details

### 7.1 `$connect`, when any connect hook is registered

1. Set the `@connections` endpoint from the event (unchanged, `src/gateway-bridge.ts:326-329`).
2. **Build the `Handshake`** (pure): lower-case header names; split `sec-websocket-protocol` into
   `subprotocols`; `queryStringParameters` without `undefined`s; `identity.sourceIp`/`userAgent`;
   `connectedAt` from `requestContext.connectedAt`, else `Date.now()`; `authorizer`.
3. **Negotiate** the subprotocol with the existing `negotiate()` (`:502-513`) and record it as
   `handshake.subprotocol`. This deliberately runs *before* the hooks, not after: it is a pure
   function of the headers and the registered protocols, and protocol-specific auth needs to know
   which protocol the socket will speak. "Nothing is persisted before authentication" still holds —
   nothing is written until step 6.
4. Create the client in phase `connecting`. It is **not** put in the instance's client map yet.
5. **Run the hooks** inside `runInDispatchScope` (so any send they enqueue is awaited) and under
   `connectTimeout`: builder hooks in registration order, then each protocol's `onConnect` in
   `orderedProtocols()` order (`:474-479`: signature-bearing protocols first, the Nest fallback last).
   The first throw stops the chain and is mapped by §5.3.
6. **Accept:** normalise and size-check `client.data`; `store.add(id, { connectedAt, subprotocol?,
   sourceIp?, data? })`; join `GLOBAL_ROOM` iff no subprotocol was negotiated (the existing rule,
   `:344-354`); apply the joins/leaves the hooks recorded, in order (§7.5); reduce the handshake;
   freeze `data`; switch to phase `open`; cache the client. Nest's `'connection'` is still emitted
   lazily, on the first frame, exactly as today.
7. Answer `200`, with `Sec-WebSocket-Protocol` when one was negotiated (unchanged).

**With no connect hook registered, steps 2-6 do not run:** the `CONNECT` branch is today's code,
unchanged — that is what keeps 2.1.0 a minor.

If the store write in step 6 fails, the existing catch-all answers `500` (`:378-382`): a connection
that cannot be recorded is not accepted.

### 7.2 Frames (`$default` and custom routes)

1. **Materialise** the client: the cached one; else — deduplicated through a per-instance map of
   pending promises, because in HTTP mode two frames of one connection can arrive concurrently —
   `store.get(id)`, build the client from `META` (`rehydrated: true`, the reduced handshake, frozen
   `data`), cache it, and emit `'connection'` with the materialising mark (Nest binds the handlers;
   the shim swallows Nest's `handleConnection`).
2. Route to the protocols exactly as today (`:371-376`).

Cost: one strongly consistent `GetItem` per (instance, connection) — not per frame. Because `data`
cannot change after accept (§5.4), the cached copy cannot be stale. `ensureClient()`
(`:436-444`) stays public and synchronous for compatibility (no rehydration); the MESSAGE path
switches to the new asynchronous `materialize()`.

### 7.3 `$disconnect` and `410 Gone`

`cleanup(id, reason)` (`:457-470`) gains a reason and a client: the cached one, or — when connect
hooks are registered — one rehydrated from `store.get` (no client when `META` is already gone; then
`handleDisconnect` is skipped, because that connection was never accepted or is already cleaned up).
Order: protocols' `onDisconnect(id, client, reason)` (unchanged position: before the store row
goes), `store.remove`, unsubscribe streams, drop the cache.

### 7.4 Errors, timeouts and the dispatch scope

- The connect phase runs in its own dispatch scope (today `CONNECT` runs outside one,
  `:337-358` vs `:365`), so `enqueueBroadcast` from a hook is honoured.
- `connectTimeout` (default 10 s) bounds the whole hook chain. API Gateway gives up on an integration
  after 29 s, and the example function's timeout is 60 s (`sst.config.ts:171`); a hook stuck on a
  dead identity provider should answer `503` well before either.
- A hook's failure never leaves partial state: data and recorded joins are discarded with the client.

### 7.5 Sends, joins and disconnects during the connect phase

- **`client.join()` / `client.leave()` are recorded, not written,** and applied in order after
  `store.add`. A rejected connection leaves no room rows behind — which matters because room rows have
  no `ttl` (§1.7).
- **`client.emit()` / `send()` / `sendRaw()` throw** a clear error ("cannot send before `$connect` is
  accepted ..."). API Gateway does not accept `PostToConnection` for a connection whose `$connect` has
  not completed (to verify, §15). Deferring them through the outbox is possible but racy: a stream
  record delivered before API Gateway opens the socket gets a `410`, and the flusher's `410` handling
  (`src/outbox.ts:270-275`) would reap the brand-new connection. Phase 2, if wanted.
- **`server.emit()` / `server.to(room).emit()`** are allowed: they are outbox writes and cannot target
  the connecting socket, which is in no room yet.
- **`client.disconnect()`** marks the connection for rejection (`403`) — the Socket.IO idiom ported
  from existing gateways keeps working.

### 7.6 Unknown connections

When connect hooks are registered, a frame whose connection has no `META` was never accepted by them
(or expired): it is not routed to any protocol, the bridge answers `403` (informational on MESSAGE
routes, `src/contract.ts:64-66`), logs it, and calls `publisher.disconnect?.(id)`. Without hooks,
today's behaviour stands (any `connectionId` is served).

---

## 8. GraphQL and `connection_init`

### Phase 1 (this plan): the same identity, in the GraphQL context

- Nest gateways' `handleConnection` and the bridge hooks run for GraphQL sockets too (§5.5, point 7),
  so a `graphql-transport-ws` socket that reaches `connection_init` has already been authenticated at
  `$connect`. `connection_init` is still acknowledged unconditionally
  (`src/graphql/handler.ts:173-178`).
- `GraphQLWsHandler` puts `connection: { id, data }` into every operation's `contextValue` — the
  subscribe path (`src/graphql/handler.ts:240`) reads it from the rehydrated client; the `context`
  option (`:88-91`) receives it as a second argument, `(connectionId, { data })`, which existing
  one-argument callbacks ignore.
- **Replays are the subtle part.** `deliver()` (`:325-373`) re-executes a stored subscription from the
  flush path, with no client and no frame, for every subscriber of every publish. Reading `META` there
  would cost one `GetItem` per subscriber per publish. Instead, the subscribe-time `data` is
  snapshotted into the registry row: `GqlSubscriptionRecord.connectionData?`
  (`src/graphql/subscription-registry.ts:29-38`), written by `add()` (`:106-131`, already a spread of
  the record) and read back by `hydrate()` (`:147-156`) and the in-memory registry. Since `data` is
  immutable after connect, the snapshot is exact. Its size counts towards every forward row
  (`maxConnectionData` keeps that bounded).
- Resolvers read it with the ordinary `@Context('connection')`.

### Phase 2 (sketch, not built): authenticate at `connection_init`

graphql-ws's own server takes `onConnect(ctx)` and closes with `4403 Forbidden` when it returns
`false`. The equivalent here:

- `GraphQLWsOptions.onConnect?(ctx: { client, payload }): boolean | Record<string, unknown> | void`
  — `false`/throw: send an `error` frame and `publisher.disconnect()` (no close codes over
  `@connections`, `src/graphql/messages.ts:20-24`); an object: merged into `client.data` and
  persisted with a new `ConnectionStore.update?(id, { data })` (`UpdateItem SET #data = :data`).
- What that requires and Phase 1 avoids: `data` becomes mutable after accept, so cached clients can be
  stale on other instances — a `subscribe` frame must read `META` again (or the bridge needs a
  per-frame refresh policy), the "acknowledged" state must be persisted (graphql-ws refuses
  `subscribe` before `connection_ack`), and registry snapshots must be taken after init.

**Recommendation for the docs:** authenticate GraphQL sockets at `$connect` — a short-lived token in
the query string, or a `Sec-WebSocket-Protocol` entry next to `graphql-transport-ws`. `connectionParams`
arrive only after API Gateway has accepted, and billed, the socket. Phase 1 and Phase 2 share the
mechanism: both write `client.data`, so guards and resolvers read one place whichever layer
authenticated.

---

## 9. The HTTP dispatch route

`ApiGatewayWsAdapterOptions` (`src/ws-adapter.ts:61-64`) gains three options; `createNestApp`
already passes `adapter` options through (`src/app-factory.ts:26-30, 43`).

- **`dispatchPath: false`** — register nothing. The Lambda examples (`src/example/src/handler.ts:38`,
  README Quick start) set it; Lambda never needs the route.
- **`dispatchSecret`** (default `process.env.APIGW_DISPATCH_SECRET`, read in `src/config.ts` like
  `APIGW_DISPATCH_PATH`) and **`dispatchSecretHeader`** (default `x-apigw-dispatch-secret`): compare
  with `crypto.timingSafeEqual` over SHA-256 digests of both values (equal lengths, no length leak);
  a mismatch answers `403` before the body is parsed and never reaches `dispatch`. API Gateway's HTTP
  integration adds the header with a request parameter mapping
  (`integration.request.header.x-apigw-dispatch-secret` = `'<secret>'`).
- **A boot-time warning** (once) when the route is registered without a secret while
  `lifecycle: 'connect'` or any connect hook is active — the combination that makes it an
  authentication bypass (§1.4).
- **Forward `result.headers`** through `httpAdapter.setHeader(res, name, value)` so HTTP mode echoes
  `Sec-WebSocket-Protocol` (§1.4); use `httpAdapter.status`/`reply` instead of `res.status`/`res.send`
  so Express and Fastify behave the same.
- **HTTP mode needs the handshake forwarded.** For `$connect`, the integration's request template
  must carry headers and query string, or the hooks see nothing. A starting point to verify on a
  deployed stage (§15):

  ```vtl
  {
    "requestContext": {
      "routeKey": "$context.routeKey", "eventType": "$context.eventType",
      "connectionId": "$context.connectionId", "domainName": "$context.domainName",
      "stage": "$context.stage", "requestId": "$context.requestId",
      "connectedAt": $context.connectedAt,
      "identity": { "sourceIp": "$context.identity.sourceIp" }
    },
    "headers": {#foreach($h in $input.params().header.keySet())"$h": "$util.escapeJavaScript($input.params().header.get($h))"#if($foreach.hasNext),#end#end},
    "queryStringParameters": {#foreach($q in $input.params().querystring.keySet())"$q": "$util.escapeJavaScript($input.params().querystring.get($q))"#if($foreach.hasNext),#end#end}
  }
  ```

- **3.0:** the route is registered only when `dispatchPath` is set explicitly.

---

## 10. Local emulator

`src/example/src/local-server.ts` must behave like API Gateway on refusal, or the feature cannot be
developed locally:

1. `new WebSocketServer({ noServer: true, handleProtocols: (offered, req) => req[NEGOTIATED] ?? false })`
   — echo what the bridge negotiated instead of the hardcoded GraphQL subprotocol (`:185-189`).
2. Handle `server.on('upgrade', ...)` itself: generate the `connectionId`, dispatch `CONNECT`
   **before** upgrading, with `headers`, `queryStringParameters` (parsed from `req.url`) and
   `requestContext.identity` (`sourceIp` from `req.socket.remoteAddress`, `userAgent`).
   - non-2xx: write `HTTP/1.1 <status> <reason>\r\nConnection: close\r\nContent-Type: text/plain\r\nContent-Length: <n>\r\n\r\n<body>`
     and end the socket — a `ws` client sees `unexpected-response` with that status, a browser a failed
     handshake;
   - 2xx: stash `res.headers['Sec-WebSocket-Protocol']` on the request, `wss.handleUpgrade(...)`,
     register the socket in `LocalSocketRegistry` (now with `close`), then emit `'connection'`;
   - if the TCP socket closed while `CONNECT` was being dispatched, dispatch `DISCONNECT` so the store
     does not keep a connection nobody holds.
3. `gwEvent` (`:98-134`) gains `queryStringParameters` and `identity` on connect.
4. Keep the `$connected` hello (`:198-200`) — it now naturally follows acceptance.
5. Dev-only `GET /__connections/:id` answering `store.get(id)` as JSON, or `404` — a test aid next to
   `POST /__reload` (`:163-176`).
6. `buildInstance()` (`:69-86`) passes `{ adapter: { lifecycle: 'connect' } }` to `createNestApp`. The
   shared `store` (`:62`) is what lets `/__reload` prove rehydration: a new bridge, the same store,
   `client.data` intact.

---

## 11. Compatibility, migration and versioning

### 11.1 What does not change in 2.1.0

- No connect hook registered and `lifecycle` left at `'legacy'`: the `CONNECT` branch, the MESSAGE
  path, the `META` row and the `/@dispatch` route are byte-for-byte today's behaviour.
  `handleConnection` keeps running lazily without a handshake, `handleDisconnect` stays uncalled.
- New port members are optional; `ProtocolHandler.onDisconnect` gains optional parameters only;
  `GatewayClient`'s constructor gains an optional argument; `ensureClient()` stays.
- The only observable additions without opting in: `client.handshake` (minimal) and `client.data`
  (`{}`) exist, and fire-and-forget client sends are awaited by the dispatch (§1.7) — a fix, not a
  behaviour change a caller could depend on.

### 11.2 Migrating a `GatewayBridge` subclass

Before (paraphrased from §1.6):

```ts
class RealtimeBridge extends GatewayBridge {
  constructor(config: BridgeConfig, private auth: Authenticator, private grants: GrantRepository) { super(config); }
  override async dispatch(event: ApiGwWsEvent) {
    if (event.requestContext.eventType === 'CONNECT') {
      const grant = await this.auth.authenticate(event.headers, event.queryStringParameters);
      if (!grant) return { statusCode: 401 };
      await this.grants.save(event.requestContext.connectionId, grant);   // orphaned if store.add then fails
    }
    return super.dispatch(event);
  }
}
// + the four presets rebuilt by hand, + ProtocolHandler.onDisconnect deleting the grant,
// + a guard loading the grant by client.connectionId on every frame, + its own table and migration.
```

After:

```ts
const bridge = GatewayBridge.builder().provider(provider).use(new PhoenixProtocol()).build();
app.useWebSocketAdapter(new ApiGatewayWsAdapter(app, bridge, { lifecycle: 'connect', dispatchPath: false }));

@WebSocketGateway()
export class IntelligenceGateway implements OnGatewayConnection {
  constructor(@Inject(Authenticator) private readonly auth: Authenticator) {}
  async handleConnection(client: GatewayClient<{ grant: Grant }>) {
    const grant = await this.auth.authenticate(client.handshake);   // headers, query.join_token, ...
    if (!grant) throw new UnauthorizedException();
    client.data.grant = grant;                                       // keep it small: claims, not tokens
  }
}
// the guard: `return !!context.switchToWs().getClient<GatewayClient<{ grant?: Grant }>>().data.grant;`
```

(If authentication belongs to the Phoenix protocol rather than the app, the same body goes in
`PhoenixProtocol.onConnect(client)`.) Drop: the subclass, the grant table, the disconnect cleanup,
the per-frame grant query, the hand-built presets. `GatewayBridgeBuilder.build(ctor?)` exists for
subclasses that remain for other reasons.

### 11.3 Rolling deploys

Connections opened before the upgrade have `META` without `data`; under `lifecycle: 'connect'` their
frames rehydrate `data = {}`, so guards refuse them and clients reconnect through the new `$connect`.
That is fail-closed by construction. A connection with no `META` at all is refused (§7.6).

### 11.4 NestJS as `peerDependencies` (3.0) — a separate, major item

**Change:** `@nestjs/common`, `@nestjs/core`, `@nestjs/websockets`: `^11.0.0 || ^12.0.0` as peers;
`rxjs: ^7.1.0` and `reflect-metadata: ^0.1.12 || ^0.2.0` as peers (Nest's own ranges, from
`@nestjs/websockets@12.1.2`'s `peerDependencies`); `@nestjs/platform-express` an **optional** peer,
with `createNestApp` gaining `httpAdapter?: AbstractHttpAdapter` (`NestFactory.create(module,
httpAdapter, options)`); optional peer `@nestjs/graphql` widened to `^12 || ^13 || ^14`;
`engines.node` to `>=20`. Nest 11 stays in `devDependencies`; CI adds a Nest 12 leg.

**Why major, not minor.** Widening a *dependency* range would be a minor, but it does not fix
anything — a hard dependency still installs its own copy whenever hoisting differs, so it only changes
which duplicate you get. Moving to peers is the fix, and it can break installs:

- a consumer that never declared `@nestjs/platform-express` and relied on `createNestApp`'s default
  Express adapter fails at boot once it is no longer installed for them;
- strict peer resolution (npm `ERESOLVE` on a Nest major outside the range, pnpm with
  `auto-install-peers=false` or `strict-peer-dependencies`, Yarn) produces new install-time errors;
- Nest 12 support means `node >= 20` (both Nest 11 and 12 declare it; this package still says
  `>=18`), and this package's CJS bundle reaching ESM-only Nest 12 relies on `require(esm)`
  (Node ≥ 20.19 or ≥ 22.12).

Ship it **together with the default flips**, which are breaking anyway — `lifecycle: 'connect'` by
default, `/@dispatch` only when `dispatchPath` is set, `ConnectionStore.get` required — so consumers
take one migration, not three.

### 11.5 Release plan

| Release | Contents |
|---|---|
| **2.1.0** (minor) | §5-§10 behind opt-ins; `build(ctor?)`; `createNestApp({ httpAdapter })`; fire-and-forget client sends awaited; tests, CI, docs |
| 2.2.0 (optional) | Phase 2: `connection_init` `onConnect`, `ConnectionStore.update?`, mutable `data` with a refresh policy, deferred connect-phase sends |
| **3.0.0** (major) | §11.4 peers; default flips; `get` required; Nest 11 + 12 CI matrix |

---

## 12. Testing plan

The repository has no unit-test runner; its suites are plain `node` scripts with a `check()` helper
(`src/example/test/outbox.e2e.mjs:23-27`) — the bridge-level one runs against the built bundle
(`:21`), the GraphQL one against the emulator (`src/example/test/graphql-subscriptions.e2e.mjs`). The
new suites follow the same style. In `.mjs` files, load `@nestjs/*` through `createRequire` (Nest 11
is CommonJS).

### 12.1 `src/example/test/connect.e2e.mjs` — the bridge, no Nest, no network

`pnpm test:connect` (`pnpm build:js && node ...`). Synthetic events; spy store/publisher wrappers
around the in-memory ones.

1. **No hooks:** `CONNECT` answers `200`, `META` has no `data`, the socket is in `GLOBAL_ROOM` —
   today's behaviour, pinned.
2. **Rejection:** a hook throwing `ConnectionRejectedError(403)` → `403`; `store.get(id)` is `null`;
   not in `GLOBAL_ROOM`; a `client.join('x')` recorded before the throw left no row.
3. **Status mapping:** `{ getStatus: () => 401 }` → `401`; `{ getError: () => ({ status: 403 }) }` →
   `403`; `{ getError: () => 'nope' }` → `401`; `{ getStatus: () => 502 }` → `500`; plain `Error` →
   `500`; a hook slower than `connectTimeout(50)` → `503`; `client.disconnect()` → `403`.
4. **Handshake:** `Authorization` arrives as `handshake.headers.authorization`; `?token=` as
   `handshake.query.token`; `Sec-WebSocket-Protocol: graphql-transport-ws, bearer.abc` → `subprotocols`
   has both, the response echoes only the registered one.
5. **Accept and persist:** `store.get(id).data` deep-equals the JSON-normalised data; a `Date` is an
   ISO string on the connecting instance's client too; writing to `client.data` afterwards throws.
6. **Rehydrate across a fresh bridge** sharing the store (the emulator's `/__reload` shape): a test
   protocol's `handleFrame` sees equal `data`, `rehydrated === true`, empty `handshake.headers`.
7. **No warm/cold asymmetry:** on the connecting bridge, after accept, `handshake.headers` is empty too.
8. **Unknown connection** with hooks: no protocol sees the frame, `403`, `publisher.disconnect` called;
   without hooks: delivered (legacy).
9. **Oversized data** → `500`, nothing stored.
10. **Connect-phase membership and sends:** `join` applied after accept (`membersOf` includes it);
    `emit` during connect throws the documented error.
11. **`$disconnect`:** `onDisconnect(id, client, reason)` sees `client.data` before `META` is gone;
    afterwards `store.get(id)` is `null`.
12. **`410`:** `flushRecord` with a `ConnectionGoneError` target → cleanup with reason `'gone'` →
    `data` gone.
13. **Concurrency:** two simultaneous MESSAGE dispatches for one connection on a fresh bridge → one
    `store.get`, one `'connection'` emission.
14. **Registration guard:** `onConnect` on a bridge whose store has no `get` throws.
15. **`build(SubBridge)`** returns a `SubBridge` with the provider preset.
16. **GraphQL snapshot:** `GraphQLWsHandler` registry rows carry `connectionData` and a replay's
    context has it without a store read (spy store: zero `get` calls during `flushRecord`).

### 12.2 `src/example/test/dispatch-route.e2e.mjs` — the HTTP route

`pnpm test:dispatch`. `createNestApp` over an empty module (`Module({})(class AppModule {})`),
`listen(0)`:

- default options: `POST /@dispatch` reaches the bridge (pinned legacy behaviour);
- `dispatchPath: false`: `404`;
- `dispatchSecret: 's'`: no header → `403`, wrong header → `403`, right header → `200`, and a `CONNECT`
  that negotiates a subprotocol returns the `Sec-WebSocket-Protocol` response header.

### 12.3 `src/example/test/auth.e2e.mjs` — the example app through the emulator

`pnpm test:auth`, with `pnpm dev` running (the example of §12.5):

1. `ws://localhost:6005/?token=bad` → `unexpected-response` with `401`; no socket registered.
2. `?token=demo:alice` → open; `chat.whoami` → `{ name: 'alice' }`; `GET /__connections/<id>` shows
   `data.user.name === 'alice'`.
3. No token → open (anonymous); `chat.send` → an `exception` frame whose `data.message` is
   `'Forbidden resource'` (the guard, through Nest's base exception filter); `post.list` still
   answers.
4. `POST /__reload`, then `chat.whoami` on alice's **existing** socket → still `alice`: `client.data`
   rehydrated on a "new Lambda instance".
5. `chat.send` from alice with `from: 'mallory'` in the body → the broadcast says `alice`.
6. Close alice's socket → `GET /__connections/<id>` → `404`.

`pnpm test:gql` must stay green unchanged — it connects without a token, which the example accepts as
anonymous — and gains one assertion: a `whoami` query over a socket opened with `?token=demo:bob`
answers `bob` through `@Context('connection')`.

### 12.4 Against AWS

`pnpm test:auth:aws` = the same script with `SKIP_RELOAD=1`, a larger `LAG`, and `WS_URL` from
`pnpm deploy` (steps 4 and 6 skipped: they use emulator-only endpoints). This run is also how the §15
questions get answered — notably which status a browser sees on refusal.

### 12.5 The example app

- `ChatGateway implements OnGatewayConnection`: `?token=demo:<name>` → `client.data.user = { name }`
  and `client.join('user:<name>')`; no token → anonymous; any other token →
  `throw new UnauthorizedException('invalid token')`.
- `src/example/src/chat/ws-user.guard.ts`: `WsUserGuard` (`!!client.data.user`), on `chat.send`, whose
  `from` comes from `client.data.user.name` instead of the client-sent field
  (`src/example/src/chat/chat.gateway.ts:89-97`); a new `chat.whoami`.
- `src/example/public/chat.html:141` connects with `?token=demo:<name>`.
- `handler.ts`: `{ lifecycle: 'connect', dispatchPath: false }`; `bootstrap.ts`:
  `{ lifecycle: 'connect' }` (secret from `APIGW_DISPATCH_SECRET`); the emulator per §10.
- Explicit `@Inject(...)` everywhere, as the rest of the example does: the Lambda bundle has no
  decorator metadata (`tsconfig.lambda.json`).

### 12.6 CI

`.github/workflows/ci.yml:22-35` runs typecheck, build and two bundle smoke tests — no suite at all.
Add, after the build: `node src/example/test/outbox.e2e.mjs`, `connect.e2e.mjs`,
`dispatch-route.e2e.mjs`; and an emulator step that starts `pnpm dev` in the background, waits for
port 6005 with a small `node` loop, and runs `pnpm test:gql` and `pnpm test:auth`. In 3.0, a matrix
leg installs Nest 12 (`@nestjs/{common,core,websockets,platform-express,platform-ws}@12`,
`@nestjs/{graphql,apollo}@14`) on Node 22 and runs the same steps.

---

## 13. Documentation changes

README sections, in order of appearance:

- **Features / Install** (`README.md:57-90`): one bullet for authentication at `$connect`; in 3.0, the
  peer dependency install line.
- **Quick start** (`:145-188`): the Lambda bootstrap gains `{ lifecycle: 'connect', dispatchPath: false }`.
- **New section "Authentication at `$connect`"**, after "Gateway API": the Nest way
  (`handleConnection` + guard, §5.2); framework-agnostic `onConnect`; protocol-level `onConnect`;
  the status table (§5.3); what is persisted and what is not (§5.4); getting a token through a
  browser, which cannot set WebSocket headers — a short-lived single-use ticket in the query string
  (query strings reach API Gateway access logs), an extra `Sec-WebSocket-Protocol` entry, or a cookie
  on a same-site custom domain; identity lifetime and kicking; per-user rooms; unknown connections;
  a `$connect` sequence diagram next to the existing one (`:726-753`).
- **Gateway API** (`:663-698`): `client.handshake`, `client.data`, `client.rehydrated`,
  `client.disconnect()`; rewrite "Connection lifecycle — handled for you" (`:694-698`) to say when
  `handleConnection`/`handleDisconnect` run in each `lifecycle` mode.
- **Protocols** (`:366-394`): `onConnect`, the new `onDisconnect` parameters.
- **GraphQL subscriptions**: `@Context('connection')`, the registry snapshot, the `$connect` vs
  `connection_init` recommendation (§8).
- **Runtime modes** (`:804-824`): HTTP mode needs `dispatchSecret` and the mapping template (§9);
  Lambda sets `dispatchPath: false`.
- **Local development** (`:827-860`): the emulator refuses upgrades, forwards the query string, and
  serves `/__connections/:id`.
- **DynamoDB data model** (`:977-1018`): the `META` row's new attributes (§6), `data` as a reserved
  word, expired rows treated as missing.
- **Configuration** (`:1020-1035`): `APIGW_DISPATCH_SECRET`; `APIGW_DISPATCH_PATH` next to
  `dispatchPath: false`.
- **Public API reference** (`:1039-1147`): `Handshake`, `ConnectHook`, `ConnectionRejectedError`,
  `isConnectionRejected`, the builder's `onConnect`/`connectTimeout`/`maxConnectionData`/`build(ctor?)`,
  `bridge.onConnect`/`bridge.disconnect`, the adapter options; the Ports snippet updated with `get?`,
  `disconnect?`, and fixed (`toRoom?` optional, `pageMembersOf?` listed).
- **Production notes & caveats** (`:1202-1225`): replace the "Framework duplication" advice
  (`:1222-1223`) with the peer dependencies in 3.0; add the connect-phase limits (no sends, the
  timeout, the data ceiling and its write cost).

---

## 14. Task checklist

Each step is small enough for one commit; "verify" says how.

**2.1.0**

1. **Contract.** Add `disconnectStatusCode`, `disconnectReason`, `authorizer` to
   `ApiGwRequestContext`, and `multiValueHeaders`/`multiValueQueryStringParameters` to `ApiGwWsEvent`
   (`src/contract.ts`). *Verify:* `pnpm typecheck`.
2. **Ports.** `SessionMeta.data?`/`sourceIp?`, `ConnectionStore.get?`, `RealtimePublisher.disconnect?`,
   `ConnectionRejectedError`, `isConnectionRejected` (`src/ports.ts`); export them from
   `src/index.ts`. *Verify:* `pnpm typecheck`; `outbox.e2e.mjs` still passes with its object-literal
   store that has no `get` (`src/example/test/outbox.e2e.mjs:84-86`).
3. **In-memory store and publisher.** `InMemoryConnectionStore.get` (clone, `null`),
   `LocalPublisher.disconnect`, `LocalSocketRegistry` entries with optional `close`
   (`src/providers/local.ts`). *Verify:* `connect.e2e.mjs` cases 5-6.
4. **DynamoDB store and publisher.** `DynamoConnectionStore.add` writes `data`/`sourceIp` without
   `undefined`s; `get` with `ConsistentRead`, key stripping and the expired-`ttl` rule;
   `ApiGatewayPublisher.disconnect` via `DeleteConnectionCommand` (`src/providers/aws.ts`).
   *Verify:* `pnpm typecheck`; on a deployed stage, `pnpm test:auth:aws` plus one `aws dynamodb
   get-item` on a `META` row.
5. **`Handshake` and `GatewayClient`.** The `Handshake` type and its builder from an event;
   `GatewayClient` with `handshake`, `data`, `rehydrated`, a phase, recorded joins/leaves, the
   connect-phase send error, `disconnect()`, the generic `TData`, and the optional constructor
   argument (`src/gateway-bridge.ts:71-116`). *Verify:* `pnpm typecheck`; `connect.e2e.mjs` cases 4,
   10.
6. **Await fire-and-forget client sends.** `send`/`sendRaw`/`emit` also `enqueueBroadcast` their
   promise (`src/gateway-bridge.ts:90-108`). *Verify:* `connect.e2e.mjs` — a protocol that calls
   `client.emit` without awaiting still has the send completed when `dispatch` resolves; `auth.e2e.mjs`
   step 3.
7. **The connect phase.** Bridge and builder `onConnect`, `connectTimeout`, `maxConnectionData`; the
   registration guard (store without `get`); the `CONNECT` branch per §7.1 with the unchanged fast
   path; status mapping per §5.3; `ProtocolHandler.onConnect` (`src/gateway-bridge.ts:186-211,
   322-383, 556-656`). *Verify:* `connect.e2e.mjs` cases 1-5, 9, 14; `outbox.e2e.mjs` green.
8. **Materialisation.** Async `materialize()` with per-instance deduplication, rehydration and the
   unknown-connection refusal; MESSAGE path switched to it; `ensureClient` kept (`:365-377, 436-444`).
   *Verify:* cases 6-8, 13.
9. **Cleanup.** `cleanup(id, reason)` with a rehydrated client; protocols' `onDisconnect(id, client,
   reason)`; reasons for `$disconnect`, `410` and `bridge.disconnect` (`:446-470`, and the flusher's
   `onConnectionGone` at `:253`). *Verify:* cases 11-12.
10. **Builder.** `build(ctor?)` (`:624-655`) and `bridge.disconnect(id)`. *Verify:* case 15.
11. **Adapter lifecycle.** `lifecycle` option; gateway discovery on first `create()`; the
    `handleConnection` shim; `NestGatewayProtocol.onConnect`/`onDisconnect`; the request-scoped boot
    error (`src/ws-adapter.ts`). *Verify:* `auth.e2e.mjs` steps 1-4; a request-scoped gateway in a
    throwaway module fails `app.init()` with the documented message.
12. **Dispatch route.** `dispatchPath: false`, `dispatchSecret`/`dispatchSecretHeader`,
    `APIGW_DISPATCH_SECRET` in `src/config.ts`, the boot warning, forwarded response headers through
    the HTTP adapter (`src/ws-adapter.ts:61-113`). *Verify:* `dispatch-route.e2e.mjs`.
13. **`createNestApp({ httpAdapter })`** (`src/app-factory.ts`). *Verify:* `dispatch-route.e2e.mjs`
    runs once with Express and once with a `FastifyAdapter` if `@nestjs/platform-fastify` is added as a
    devDependency (else Express only).
14. **GraphQL, phase 1.** `connection` in the operation context, the `context` callback's second
    argument, `GqlSubscriptionRecord.connectionData` written and hydrated (`src/graphql/handler.ts`,
    `src/graphql/subscription-registry.ts`). *Verify:* case 16; the `whoami` assertion in `test:gql`.
15. **Emulator.** §10 (`src/example/src/local-server.ts`). *Verify:* `pnpm test:gql` unchanged and
    green; `pnpm test:auth` steps 1 and 6.
16. **Example app.** §12.5. *Verify:* `pnpm typecheck`; `pnpm test:auth`; by hand, two browser tabs on
    `/chat` with different names see server-attributed `from`.
17. **Scripts.** `test:connect`, `test:dispatch`, `test:auth`, `test:auth:aws` in `package.json:59-77`.
    *Verify:* each runs locally.
18. **CI.** §12.6 for 2.x (`.github/workflows/ci.yml`). *Verify:* the pull request's checks run every
    suite.
19. **README.** §13. *Verify:* every new export appears in the Public API reference; links resolve.
20. **Release 2.1.0** (the maintainer): bump `version`, tag, GitHub Release; `publish.yml` publishes.

**3.0.0**

21. **Peers.** §11.4 in `package.json`; `pnpm install`; lockfile updated. *Verify:* `pnpm why
    @nestjs/core` in a scratch Nest 12 + Fastify app installing the packed tarball shows one copy.
22. **Defaults.** `lifecycle: 'connect'`, `dispatchPath` unset = no route, `ConnectionStore.get`
    required. *Verify:* `connect.e2e.mjs` case 1 and `dispatch-route.e2e.mjs` updated to the new
    defaults; a store without `get` fails to compile.
23. **Nest 12 CI leg** (§12.6). *Verify:* green on Nest 11 / Node 20 and Nest 12 / Node 22.
24. **Migration guide** in the README (2.x → 3.0: peers, defaults, Node 20).

---

## 15. Open questions and things to verify on AWS

1. **Which status does the client see when the `$connect` integration answers non-2xx?** The plan
   assumes API Gateway forwards the Lambda's status (so `401` vs `403` is meaningful to a `ws`
   client). Verify with `pnpm test:auth:aws`; if it is always the same, the status still matters for
   logs and the emulator, and the docs say so.
2. **Is `PostToConnection` refused while `$connect` is in flight** (assumed `410`)? Decides whether
   connect-phase sends could ever be deferred (§7.5).
3. **HTTP integration:** does `$input.params().header` expose every handshake header in a WebSocket
   request template, and do integration response headers (the `Sec-WebSocket-Protocol` echo) reach the
   client on `$connect`? The mapping template in §9 is a starting point, not a tested artefact.
4. **`requestContext.authorizer`** on MESSAGE and `$disconnect` events when a Lambda authorizer guards
   `$connect` — assumed present, per API Gateway's documentation of `$context.authorizer.*`.
5. **`disconnectStatusCode` / `disconnectReason`** field names on `$disconnect` events — assumed from
   `$context.disconnectStatusCode`/`$context.disconnectReason`.
6. **Does `DeleteConnection` produce a `$disconnect` event?** Assumed yes; if not, `bridge.disconnect`
   must call `cleanup` itself.
7. **Lambda's `nodejs20.x` runtime and `require(esm)`** for the CJS bundle under Nest 12 (3.0): check
   `process.version` ≥ 20.19 on the runtime, or document `nodejs22.x`.
8. **Lazily loaded modules** (`LazyModuleLoader`): gateways there are not connected by Nest either, so
   discovery missing them is consistent — confirm and document.
9. **Future:** let `bridge.serve()` recognise an API Gateway REQUEST authorizer event and answer it
   with the same connect hooks (Allow/Deny plus `client.data` flattened into `context`), so option D
   comes for free. Out of scope here.
