# Changelog

All notable changes to `apigw-ws-nest`. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/).

## 3.0.0 — 2026-10-09

`$connect` becomes a decision: a connection can be refused with a status of your choosing, and the
identity established there is persisted with the connection and rehydrated on every instance — so a
plain Nest guard works anywhere. See [Upgrading from 2.x](./README.md#upgrading-from-2x) for what
to change.

### Breaking

- **NestJS is a peer dependency.** `@nestjs/common`, `@nestjs/core` and `@nestjs/websockets`
  (`^11.0.0 || ^12.0.0`), `rxjs` (`^7.1.0`) and `reflect-metadata` (`^0.1.12 || ^0.2.0`) moved from
  `dependencies` to `peerDependencies`; `@nestjs/platform-express` is an optional peer. The library
  now always runs on the app's own copy of Nest.
- **Node ≥ 20** (was ≥ 18). The CommonJS build next to Nest 12 relies on `require(esm)`
  (Node ≥ 20.19 or ≥ 22.12).
- **`ApiGatewayWsAdapter`'s `lifecycle` defaults to `'connect'`.** `handleConnection` runs once, at
  `$connect`, awaited — throwing refuses the socket — instead of lazily on each instance's first
  frame; it cannot send (there is no socket yet); `handleDisconnect` is now called. `'legacy'`
  restores the 2.x behaviour.
- **The HTTP dispatch route is off by default.** It is registered only when `dispatchPath` is set
  (conventionally `DISPATCH_PATH`); `APIGW_DISPATCH_PATH` alone no longer registers it.
- **`ConnectionStore.get()` is required.** It must be strongly consistent with `add()` and return
  `null` for an unknown or expired connection. A store without it fails to compile, and at boot when
  it reaches a connect hook anyway.
- **Frames from unknown connections are refused** (`403`, socket closed) whenever `$connect` runs
  hooks — which is the default with the Nest adapter.
- `client.send()` / `emit()` / `sendRaw()` throw while `$connect` is being decided.

### Added

- **Authentication at `$connect`.** `OnGatewayConnection.handleConnection(client)` is awaited at
  `$connect`, with `client.handshake` (headers, query, offered and negotiated subprotocols, source
  IP, user agent, `connectedAt`, authorizer context). Whatever it writes to `client.data` is
  persisted with the connection — JSON-normalised, size-capped, deep-frozen after accept — and
  rehydrated on any instance with one consistent read per (instance, connection).
  `OnGatewayDisconnect.handleDisconnect(client, reason)` runs on `$disconnect`, `410 Gone` and
  `bridge.disconnect()`.
- Framework-agnostic connect hooks: `bridge.onConnect(hook)` and the builder's `.onConnect()`,
  `.connectTimeout()` (default 10 s → `503`), `.maxConnectionData()` (default 16 KiB), and
  `.build(SubBridge)`.
- `ProtocolHandler.onConnect(client, event)`; `onDisconnect` now also receives the client and the
  reason.
- Refusals by shape: `ConnectionRejectedError(status)`, a Nest `HttpException` (4xx), a
  `WsException` (with an optional 4xx `status`), `client.disconnect()` (`403`), a timeout (`503`),
  anything else (`500`, fail closed). New `isConnectionRejected()`.
- `GatewayClient`: `handshake`, `data` (typed with `GatewayClient<TData>`), `rehydrated`, `phase`,
  `disconnect()`. `GatewayBridge`: `disconnect(id)` (kick), `materialize(id)`, `hasConnectHooks`.
  `handshakeOf(event)`.
- `RealtimePublisher.disconnect()` — `DeleteConnection` on AWS, closing the socket locally.
- `SessionMeta.data` and `SessionMeta.sourceIp`, stored in the `META` row (expired rows are treated
  as missing).
- Dispatch route: `dispatchSecret` / `dispatchSecretHeader` (default `APIGW_DISPATCH_SECRET`,
  `x-apigw-dispatch-secret`), compared in constant time; a boot warning when the route is open in
  front of connect hooks. New exports `DISPATCH_SECRET`, `DISPATCH_SECRET_HEADER`.
- `createNestApp(..., { httpAdapter })` — Fastify supported and tested.
- NestJS 12, `@nestjs/graphql` 14 and `graphql-ws` 6 supported.
- GraphQL: every operation's context carries `connection: { id, data }` (`@Context('connection')`);
  the `context` option receives `(connectionId, { data })`; subscription rows snapshot the
  connection's data (`connectionData`), so a replay needs no store read.
- Contract: `requestContext.disconnectStatusCode`, `disconnectReason`, `authorizer`;
  `multiValueHeaders`, `multiValueQueryStringParameters`.

### Fixed

- A send nobody awaited — notably Nest's exception filter answering a guard's refusal — could be
  frozen mid-flight on Lambda; client sends are now awaited by the dispatch that made them.
- The HTTP dispatch route dropped the bridge's response headers, so the `$connect`
  `Sec-WebSocket-Protocol` echo never left the process. Status, body and headers now go through the
  HTTP adapter, alike on Express and Fastify.
- A `$connect` that failed after its `META` write left a half-recorded connection behind.

### Example app, tooling and docs

- The emulator refuses the upgrade with the `$connect` status, forwards the query string and the
  caller's address, echoes the negotiated subprotocol, serves `GET /__connections/:id`, and is a
  function, `startEmulator({ port })`.
- The example authenticates `?token=demo:<name>` in `ChatGateway.handleConnection`, guards
  `chat.send` (whose `from` is now the authenticated name), and answers `chat.whoami` and a GraphQL
  `whoami`.
- SST: `nodejs22.x`, and an `import.meta.url` shim so the CommonJS bundle of a Nest 12 app boots.
- A Vitest suite — unit, integration (Express and Fastify) and end-to-end through the emulator —
  replaces the ad-hoc scripts, with coverage thresholds. CI runs it on Nest 12 and Nest 11 (Node 22);
  publishing runs it first.
- README: authentication at `$connect`, testing, and the upgrade guide.

## 2.0.0 and earlier

See the [git history](https://github.com/Manuel-Antunes/nestjs-websockets-api-gateway-platform/commits/main).
