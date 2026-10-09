/* =============================================================================
 *  What the end-to-end suites run against.
 * =============================================================================
 *  By default, a local emulator of their own on a free port — the example app,
 *  real WebSockets, the same code path API Gateway drives. Set E2E_WS_URL to run
 *  the same suites against a deployed API instead:
 *
 *    E2E_WS_URL='wss://<api-id>.execute-api.<region>.amazonaws.com/$default' pnpm test:e2e
 *
 *  Steps that need the emulator's dev endpoints (/__reload, /__connections) are
 *  skipped then. E2E_LAG is how long to wait for a frame that should NOT arrive
 *  (and the scale of every other wait): a few hundred ms locally, seconds
 *  against a deployed stage.
 * ========================================================================== */

import WebSocket from 'ws';

export const REMOTE = process.env.E2E_WS_URL;
export const LAG = Number(process.env.E2E_LAG ?? (REMOTE ? 8000 : 300));

export interface Target {
  wsUrl: string;
  /** Emulator only. */
  httpUrl?: string;
  close(): Promise<void>;
}

export async function startTarget(): Promise<Target> {
  if (REMOTE) return { wsUrl: REMOTE, close: async () => {} };
  const { startEmulator } = await import('../../src/example/src/emulator');
  const emulator = await startEmulator({
    port: 0,
    verbose: false,
    platform: process.env.E2E_PLATFORM === 'fastify' ? 'fastify' : 'express',
  });
  return { wsUrl: emulator.wsUrl, httpUrl: emulator.httpUrl, close: () => emulator.close() };
}

/** POST /__reload: a brand-new Nest app + bridge over the same store. */
export async function reload(target: Target): Promise<number> {
  const res = await fetch(`${target.httpUrl}/__reload`, { method: 'POST' });
  return (await res.json()).instance;
}

export const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

export interface Frame {
  event: string;
  data: any;
}

/** A `{ event, data }` WebSocket client. */
export interface Socket {
  ws: WebSocket;
  frames: Frame[];
  connectionId?: string;
  send(event: string, data?: unknown): void;
  /** The next frame named `event`, received at or after index `from`. */
  next(event: string, from?: number): Promise<Frame>;
  close(): Promise<void>;
}

/** Open a socket; resolves with it, or with the status the handshake was refused with. */
export function open(target: Target, query = ''): Promise<Socket | { refused: number }> {
  return new Promise(resolve => {
    const ws = new WebSocket(`${target.wsUrl}${query}`);
    const frames: Frame[] = [];
    const socket: Socket = {
      ws,
      frames,
      send: (event, data = {}) => ws.send(JSON.stringify({ event, data })),
      next: async (event, from = 0) => {
        const deadline = Date.now() + LAG * 20;
        while (Date.now() < deadline) {
          const hit = frames.slice(from).find(f => f.event === event);
          if (hit) return hit;
          await sleep(10);
        }
        throw new Error(`no "${event}" frame within ${LAG * 20}ms; got ${frames.map(f => f.event).join(', ')}`);
      },
      close: () =>
        new Promise(done => {
          if (ws.readyState === WebSocket.CLOSED) return done();
          ws.once('close', () => done());
          ws.close();
        }),
    };
    ws.on('message', raw => {
      const frame = JSON.parse(raw.toString());
      if (frame.event === '$connected') socket.connectionId = frame.data.connectionId;
      frames.push(frame);
    });
    ws.on('unexpected-response', (_req, res) => resolve({ refused: res.statusCode ?? 0 }));
    ws.on('error', () => {});
    ws.on('open', () => resolve(socket));
  });
}

export function opened(socket: Socket | { refused: number }): Socket {
  if ('refused' in socket) throw new Error(`the handshake was refused with ${socket.refused}`);
  return socket;
}
