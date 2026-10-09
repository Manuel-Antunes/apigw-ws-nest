import { afterEach, describe, expect, it, vi } from 'vitest';
import { handshakeOf } from '../../../src';
import { connectEvent } from '../../helpers/events';

afterEach(() => vi.useRealTimers());

describe('handshakeOf', () => {
  it('lower-cases header names and drops undefined values', () => {
    const { headers } = handshakeOf(
      connectEvent('a', { headers: { Authorization: 'Bearer x', 'X-Empty': undefined } }),
    );
    expect(headers).toEqual({ authorization: 'Bearer x' });
  });

  it('falls back to multiValueHeaders, joined — the single-value form wins', () => {
    const { headers } = handshakeOf(
      connectEvent('a', {
        headers: { Host: 'h' },
        multiValueHeaders: { Host: ['ignored'], Accept: ['a', 'b'], Empty: [], None: undefined },
      }),
    );
    expect(headers).toEqual({ host: 'h', accept: 'a, b' });
  });

  it('reads the query string, the single-value form winning, else the last of several', () => {
    const { query } = handshakeOf(
      connectEvent('a', {
        queryStringParameters: { token: 't', gone: undefined },
        multiValueQueryStringParameters: { token: ['x'], tag: ['1', '2'], none: [] },
      }),
    );
    expect(query).toEqual({ token: 't', tag: '2' });
  });

  it('lists every offered subprotocol, in order', () => {
    const { subprotocols } = handshakeOf(
      connectEvent('a', { headers: { 'Sec-WebSocket-Protocol': ' graphql-transport-ws ,, bearer.abc ' } }),
    );
    expect(subprotocols).toEqual(['graphql-transport-ws', 'bearer.abc']);
    expect(handshakeOf(connectEvent('a')).subprotocols).toEqual([]);
  });

  it('takes the caller from requestContext.identity, else the user-agent header', () => {
    const fromIdentity = handshakeOf(connectEvent('a'));
    expect(fromIdentity).toMatchObject({ sourceIp: '203.0.113.7', userAgent: 'test-agent' });
    const fromHeader = handshakeOf(
      connectEvent('a', { requestContext: { identity: {} }, headers: { 'User-Agent': 'curl' } }),
    );
    expect(fromHeader.sourceIp).toBeUndefined();
    expect(fromHeader.userAgent).toBe('curl');
  });

  it('takes connectedAt from the event, else now', () => {
    expect(handshakeOf(connectEvent('a')).connectedAt).toBe(1_700_000_000_000);
    vi.useFakeTimers({ now: 42, toFake: ['Date'] });
    expect(handshakeOf(connectEvent('a', { requestContext: { connectedAt: undefined } })).connectedAt).toBe(42);
  });

  it('carries an authorizer context, and leaves the subprotocol to negotiation', () => {
    const handshake = handshakeOf(connectEvent('a', { requestContext: { authorizer: { principalId: 'u1' } } }));
    expect(handshake.authorizer).toEqual({ principalId: 'u1' });
    expect(handshake.subprotocol).toBeUndefined();
  });
});
