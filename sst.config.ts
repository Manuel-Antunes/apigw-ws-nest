/// <reference path="./.sst/platform/config.d.ts" />

/* =============================================================================
 *  SST infrastructure — API Gateway WebSocket ⇄ NestJS, deployed to AWS.
 * =============================================================================
 *  Resources (us-east-1):
 *    - Dynamo "Connections" : connection registry, room interest map, GraphQL
 *                             subscription rows, delivery ledger (pk/sk, TTL)
 *    - Dynamo "Messages"    : the OUTBOX — one row per publish, with a STREAM
 *    - Dynamo "Posts"        : the feed (plain table)
 *    - Dynamo "Chat"         : conversations + messages, single-table (pk/sk)
 *    - ApiGatewayWebSocket   : $connect/$disconnect/$default -> handler.handler
 *    - StaticSite "Web"      : the test client (public/), injected with the wss URL
 *
 *  ONE Lambda function, FOUR triggers:
 *
 *    $connect / $disconnect / $default  ->  Gateway  <-  the Messages stream
 *
 *  SST would happily create a Function per route plus one for the stream — four
 *  in total, each with its own cold pool. They all need the SAME booted Nest app
 *  (fan-out re-executes a subscription per subscriber, so it needs the schema and
 *  the DI container), so splitting them multiplies the ~0.5s boot instead of
 *  amortising it: the flush function was paying it on 13.5% of invocations,
 *  moments after a different container had finished the very mutation that
 *  produced the record.
 *
 *  So the function is created once and every trigger points at its ARN. Passing
 *  an ARN means SST does NOT attach its usual permissions (it doesn't own the
 *  function), hence the explicit stream-read grant below — the one thing that is
 *  easy to forget and fails at runtime, not at deploy.
 *
 *  Publishing is a PutItem into Messages; the stream then hands the topic back to
 *  this same function, which pages its subscribers and delivers them in batches.
 *  Lambda redelivers a stream record until it is reported successful, so a
 *  delivery can no longer be lost by the invocation that happened to publish it.
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
      // Nest 12 is ESM-only; 22.x is the runtime it (and require(esm)) is at home on.
      args.runtime ??= 'nodejs22.x';
      args.nodejs = {
        // The handler we ship is tsc's CommonJS output (see tsconfig.lambda.json).
        // SST defaults to bundling as ESM, and a CJS entry bundled to ESM exposes
        // only `default` — Lambda then fails with
        // `Function "handler" not found in "handler". Found default`.
        format: 'cjs',
        esbuild: {
          // Bundling Nest 12's ESM into CommonJS leaves `import.meta` empty, and
          // @nestjs/graphql calls createRequire(import.meta.url) at load time —
          // a cold start would die with "The argument 'filename' must be a file
          // URL ... Received undefined". Give it this file's URL instead.
          define: { 'import.meta.url': '__import_meta_url' },
          banner: { js: "const __import_meta_url = require('url').pathToFileURL(__filename).href;" },
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

    // Everything about WHO is connected and what they want: connection rows, room
    // membership, GraphQL subscription rows, and the delivery ledger. Deliberately
    // NOT streamed — it is written on every connect/join/subscribe, so a stream
    // here would be almost entirely noise.
    const connections = new sst.aws.Dynamo('Connections', {
      fields: { pk: 'string', sk: 'string' },
      primaryIndex: { hashKey: 'pk', rangeKey: 'sk' },
      // $disconnect is best-effort, and a ledger row is garbage once its message
      // can no longer be redelivered. Both write `ttl`; without this the attribute
      // was inert and those rows accumulated forever.
      ttl: 'ttl',
    });

    // The OUTBOX. One row per publish, and the row's own INSERT is the trigger
    // that delivers it. pk is `TOPIC#<topic>` / `ROOM#<room>`, which makes the
    // topic simultaneously the fan-out unit, the DynamoDB partition, and the
    // stream's ordering unit — two publishes to one topic are delivered in the
    // order they were written.
    const messages = new sst.aws.Dynamo('Messages', {
      fields: { pk: 'string', sk: 'string' },
      primaryIndex: { hashKey: 'pk', rangeKey: 'sk' },
      // new-image: the flush consumer needs the payload, not the diff.
      stream: 'new-image',
      ttl: 'expiresAt',
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
      MESSAGES_TABLE: messages.name,
      POSTS_TABLE: posts.name,
      CHAT_TABLE: chat.name,
    };
    // Where a message goes after Lambda has retried it to exhaustion. Without
    // this, "retried until it succeeds" quietly becomes "dropped after 24h" —
    // the same silent loss the outbox was built to remove, just slower.
    const dlq = new sst.aws.Queue('FlushDlq');

    // THE function. Created once, then referenced by ARN from all four triggers.
    const gateway = new sst.aws.Function('Gateway', {
      // The COMPILED handler, not the .ts source. @nestjs/graphql's @Args()
      // needs `design:paramtypes`, which only tsc emits — esbuild (SST's
      // bundler) never does, and the schema build then throws at cold start,
      // surfacing as a 502 on the WebSocket handshake. `pnpm build:lambda`
      // produces this; `pnpm live` / `pnpm deploy` run it for you.
      // See tsconfig.lambda.json.
      handler: '.lambda/example/src/handler.handler',
      // link grants IAM for each resource (DynamoDB CRUD on the tables,
      // execute-api:ManageConnections on the api). `chat` MUST be here or its
      // PutItem/Query throw AccessDeniedException; `messages` likewise, since
      // every broadcast is now a write to it.
      link: [connections, messages, posts, chat, api, dlq],
      environment: {
        ...env,
        // A stream event carries no requestContext, so there is no domainName to
        // derive the @connections endpoint from. Records normally carry it, and
        // this is the fallback for any that don't.
        MANAGEMENT_ENDPOINT: api.managementEndpoint,
      },
      // Long enough for a fan-out to walk several pages, short enough that a
      // stuck $connect can't burn five minutes. The Flusher requeues the
      // remainder below `reserveMs` (15s) rather than dying mid-topic, so this is
      // a ceiling on one invocation, not a budget for the whole topic.
      timeout: '60 seconds',
      // A subscriber created from FunctionArgs gets these from SST; referenced by
      // ARN it does not, and the failure is a runtime AccessDenied on the event
      // source mapping rather than a deploy error.
      permissions: [
        {
          actions: [
            'dynamodb:DescribeStream',
            'dynamodb:GetRecords',
            'dynamodb:GetShardIterator',
            'dynamodb:ListStreams',
          ],
          resources: [messages.nodes.table.streamArn],
        },
      ],
    });

    // Every WebSocket route -> the same function. (route() still creates the
    // lambda:InvokeFunction permission for API Gateway when given an ARN.)
    api.route('$connect', gateway.arn);
    api.route('$disconnect', gateway.arn);
    api.route('$default', gateway.arn);

    // ...and so does the outbox stream. bridge.serve() tells the two apart.
    messages.subscribe(
      'Flush',
      gateway.arn,
      {
        // A publish is the only INSERT here; everything else on this stream is a
        // TTL expiry (REMOVE). Filtering server-side means those never become a
        // billed invocation that immediately returns.
        filters: [{ eventName: ['INSERT'] }],
        transform: {
          eventSourceMapping: (args) => {
            // The handler returns { batchItemFailures }. Without declaring it
            // here that return value is IGNORED and any failure re-runs the whole
            // batch — which is both wasteful and, for a partially-delivered
            // topic, the thing the delivery ledger then has to clean up after.
            args.functionResponseTypes = ['ReportBatchItemFailures'];
            // One record can be an entire topic's fan-out, so keep batches small.
            args.batchSize = 10;
            // On error, halve the batch to isolate the poison record instead of
            // letting it hold its neighbours hostage.
            args.bisectBatchOnFunctionError = true;
            // Concurrent batches per shard. Ordering is preserved per partition
            // key — i.e. per topic — which is the only ordering we promise.
            args.parallelizationFactor = 10;
            args.destinationConfig = { onFailure: { destinationArn: dlq.arn } };
          },
        },
      },
    );

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
