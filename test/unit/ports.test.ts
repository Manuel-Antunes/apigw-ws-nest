import { describe, expect, it } from 'vitest';
import {
  ConnectionGoneError,
  ConnectionRejectedError,
  EVENT_TYPE,
  ROUTE,
  isConnectionGone,
  isConnectionRejected,
} from '../../src';

describe('ConnectionGoneError', () => {
  it('names the connection', () => {
    const err = new ConnectionGoneError('abc');
    expect(err).toBeInstanceOf(Error);
    expect(err.name).toBe('ConnectionGoneError');
    expect(err.connectionId).toBe('abc');
    expect(err.message).toBe('connection gone: abc');
  });

  it('is recognised by name, so a duplicated bundle copy still matches', () => {
    expect(isConnectionGone(new ConnectionGoneError('x'))).toBe(true);
    expect(isConnectionGone({ name: 'ConnectionGoneError' })).toBe(true);
    expect(isConnectionGone(new Error('nope'))).toBe(false);
    expect(isConnectionGone(null)).toBe(false);
    expect(isConnectionGone(undefined)).toBe(false);
  });
});

describe('ConnectionRejectedError', () => {
  it('defaults to 401', () => {
    const err = new ConnectionRejectedError();
    expect(err.name).toBe('ConnectionRejectedError');
    expect(err.statusCode).toBe(401);
    expect(err.message).toBe('connection rejected');
  });

  it('carries a chosen status and message', () => {
    const err = new ConnectionRejectedError(429, 'slow down');
    expect(err.statusCode).toBe(429);
    expect(err.message).toBe('slow down');
  });

  it('is recognised by name', () => {
    expect(isConnectionRejected(new ConnectionRejectedError())).toBe(true);
    expect(isConnectionRejected({ name: 'ConnectionRejectedError' })).toBe(true);
    expect(isConnectionRejected(new ConnectionGoneError('x'))).toBe(false);
    expect(isConnectionRejected(0)).toBe(false);
  });
});

describe('contract constants', () => {
  it('match what API Gateway sends', () => {
    expect(EVENT_TYPE).toEqual({ CONNECT: 'CONNECT', MESSAGE: 'MESSAGE', DISCONNECT: 'DISCONNECT' });
    expect(ROUTE).toEqual({ CONNECT: '$connect', DISCONNECT: '$disconnect', DEFAULT: '$default' });
  });
});
