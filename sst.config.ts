/// <reference path="./.sst/platform/config.d.ts" />

/* =============================================================================
 *  SST infrastructure — API Gateway WebSocket ⇄ NestJS, deployed to AWS.
 * =============================================================================
 *  Resources (us-east-1):
 *    - Dynamo "Connections" : connection registry + room interest map (pk/sk)
 *    - Dynamo "Posts"        : the feed (plain table)
 *    - Dynamo "Chat"         : conversations + messages, single-table (pk/sk)
 *    - ApiGatewayWebSocket   : $connect/$disconnect/$default -> src/handler
 *    - StaticSite "Web"      : the test client (public/), injected with the wss URL
 *
 *  ONE handler (src/handler) backs every WS route. The realtime fan-out happens
 *  inline in the gateway (server.to(room).emit) via the @connections Management
 *  API — no DynamoDB Streams, no stream subscriber. Functions are linked to the
 *  tables + api for DynamoDB + execute-api:ManageConnections IAM; env supplies
 *  table names + RT_PROVIDER=aws.
 * ========================================================================== */

export default $config({
  app(input) {
    return {
      name: 'apigw-ws-nest',
      removal: input?.stage === 'production' ? 'retain' : 'remove',
      home: 'aws',
      providers: { aws: { region: 'us-east-1' } },
    };
  },
  async run() {
    // esbuild defaults for every Lambda. NestJS lazily require()s optional
    // integrations that we don't use — mark them external so bundling doesn't
    // fail trying to resolve them.
    $transform(sst.aws.Function, (args) => {
      args.runtime ??= 'nodejs20.x';
      args.nodejs = {
        // The handler we ship is tsc's CommonJS output (see tsconfig.lambda.json).
        // SST defaults to bundling as ESM, and a CJS entry bundled to ESM exposes
        // only `default` — Lambda then fails with
        // `Function "handler" not found in "handler". Found default`.
        format: 'cjs',
        esbuild: {
          // Truly-optional NestJS integrations we don't use. NOTE: do NOT add
          // '@nestjs/websockets/socket-module' here — core require()s it to
          // enable WebSocket gateways, so it must be BUNDLED, not external.
          external: [
            '@nestjs/microservices',
            '@nestjs/platform-socket.io',
            '@grpc/grpc-js',
            '@grpc/proto-loader',
            'mqtt',
            'nats',
            'ioredis',
            'amqplib',
            'amqp-connection-manager',
            'kafkajs',
            'class-transformer',
            'class-validator',
            'cache-manager',
            // @nestjs/graphql + @nestjs/apollo probe for adapters and features
            // we don't use (fastify, federation, the ts-morph plugin). They're
            // require()d behind feature checks, so leaving them unresolved is
            // safe — but esbuild fails the build unless they're external.
            '@as-integrations/fastify',
            '@apollo/subgraph',
            '@apollo/subgraph/package.json',
            '@apollo/subgraph/dist/directives',
            '@apollo/gateway',
            'ts-morph',
          ],
        },
        ...(args.nodejs ?? {}),
      };
    });

    const connections = new sst.aws.Dynamo('Connections', {
      fields: { pk: 'string', sk: 'string' },
      primaryIndex: { hashKey: 'pk', rangeKey: 'sk' },
    });

    const posts = new sst.aws.Dynamo('Posts', {
      fields: { id: 'string' },
      primaryIndex: { hashKey: 'id' },
    });
    
    // Single-table layout (see DynamoConversationRepository): pk/sk, NOT a plain
    // `id` key. Conversations live under pk='CONVO' (queryable as a directory),
    // each conversation's messages under pk='CONVO#<id>' sorted by sk.
    const chat = new sst.aws.Dynamo('Chat', {
      fields: { pk: 'string', sk: 'string' },
      primaryIndex: { hashKey: 'pk', rangeKey: 'sk' },
    });

    const api = new sst.aws.ApiGatewayWebSocket('Api');

    const env = {
      RT_PROVIDER: 'aws',
      CONNECTIONS_TABLE: connections.name,
      POSTS_TABLE: posts.name,
      CHAT_TABLE: chat.name,
    };
    const route = {
      // The COMPILED handler, not the .ts source. @nestjs/graphql's @Args()
      // needs `design:paramtypes`, which only tsc emits — esbuild (SST's
      // bundler) never does, and the schema build then throws at cold start,
      // surfacing as a 502 on the WebSocket handshake. `pnpm build:lambda`
      // produces this; `pnpm live` / `pnpm deploy` run it for you.
      // See tsconfig.lambda.json.
      handler: '.lambda/example/src/handler.handler',
      // link grants the Lambda IAM for each resource (DynamoDB CRUD on the tables,
      // execute-api:ManageConnections on the api). `chat` MUST be here or its
      // PutItem/Query throw AccessDeniedException.
      link: [connections, posts, chat, api],
      environment: env,
    };

    // All WS routes share one handler.
    api.route('$connect', route);
    api.route('$disconnect', route);
    api.route('$default', route);

    const web = new sst.aws.StaticSite('Web', {
      path: 'src/example/public',
      environment: { WS_URL: api.url },
      build: {
        // Write config.js so the static client learns the deployed wss:// URL.
        command:
          'node -e "require(\'fs\').writeFileSync(\'config.js\',\'window.__WS_URL__=\'+JSON.stringify(process.env.WS_URL)+\';\')"',
        output: '.',
      },
    });

    return {
      wsUrl: api.url,
      managementEndpoint: api.managementEndpoint,
      siteUrl: web.url,
    };
  },
});
