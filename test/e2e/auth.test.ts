/* =============================================================================
 *  Authentication at $connect — the example app, end to end.
 * =============================================================================
 *  Real `ws` clients. The example's ChatGateway accepts `?token=demo:<name>`,
 *  treats no token as anonymous and refuses anything else with
 *  UnauthorizedException; WsUserGuard lets only identified connections
 *  chat.send. Against the emulator, /__reload (a new "Lambda instance" over the
 *  same store) and /__connections/<id> (what the store holds) prove that
 *  client.data is persisted and rehydrated — not merely cached.
 * ========================================================================== */

import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { LAG, REMOTE, Socket, Target, open, opened, reload, sleep, startTarget } from './target';

const RUN = Math.random().toString(36).slice(2, 8);
const ALICE = `alice-${RUN}`;

let target: Target;
let alice: Socket;
let anon: Socket;

const stored = async (id: string) => {
  const res = await fetch(`${target.httpUrl}/__connections/${encodeURIComponent(id)}`);
  return { status: res.status, body: await res.json() };
};

beforeAll(async () => {
  target = await startTarget();
});
afterAll(async () => {
  await alice?.close();
  await anon?.close();
  await target?.close();
});

describe('authentication at $connect', () => {
  it.skipIf(REMOTE)('runs on the requested HTTP platform', () => {
    expect(target.httpPlatform!()).toBe(process.env.E2E_PLATFORM === 'fastify' ? 'fastify' : 'express');
  });

  it('refuses an invalid token with 401, at the handshake', async () => {
    expect(await open(target, '?token=bad')).toEqual({ refused: 401 });
  });

  it('opens with a valid token, and knows who it is', async () => {
    alice = opened(await open(target, `?token=demo:${ALICE}`));
    alice.send('chat.whoami');
    expect((await alice.next('chat.whoami')).data).toEqual({ name: ALICE });
  });

  it.skipIf(REMOTE)('stores client.data with the connection', async () => {
    await sleep(LAG / 4);
    const { body } = await stored(alice.connectionId!);
    expect(body.data).toEqual({ user: { name: ALICE } });
  });

  it('opens anonymously without a token — reading allowed, sending refused by the guard', async () => {
    anon = opened(await open(target));
    anon.send('chat.send', { conversationId: 'nope', text: 'hi' });
    expect((await anon.next('exception')).data).toMatchObject({ status: 'error', message: 'Forbidden resource' });
    anon.send('post.list');
    expect(Array.isArray((await anon.next('post.list')).data)).toBe(true);
  });

  it.skipIf(REMOTE)('keeps the identity on a new instance: the same socket is still alice', async () => {
    expect(await reload(target)).toBeGreaterThan(1);
    const mark = alice.frames.length;
    alice.send('chat.whoami');
    expect((await alice.next('chat.whoami', mark)).data).toEqual({ name: ALICE });
  });

  it('attributes a message to who you connected as, not to what you claim', async () => {
    alice.send('chat.create', { title: `room ${RUN}` });
    const created = await alice.next('chat.created');
    alice.send('chat.join', { conversationId: created.data.id });
    await alice.next('chat.joined');
    alice.send('chat.send', { conversationId: created.data.id, from: 'mallory', text: 'it was me' });
    expect((await alice.next('chat.message')).data).toMatchObject({ from: ALICE, text: 'it was me' });
  });

  it.skipIf(REMOTE)('forgets the identity once the socket closes', async () => {
    const id = alice.connectionId!;
    await alice.close();
    await expect.poll(async () => (await stored(id)).status, { timeout: LAG * 10 }).toBe(404);
  });
});
