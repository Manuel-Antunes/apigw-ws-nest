/* =============================================================================
 *  Local-mode implementations (in-memory)
 * =============================================================================
 *  In-memory stand-ins so the whole flow runs on your machine with no AWS. The
 *  local emulator feeds events in; the LocalSocketRegistry carries pushes back
 *  out to live browser sockets. Instantiated directly (see runtime.ts), not via
 *  the Nest DI container.
 * ========================================================================== */

import { ConnectionStore, RealtimePublisher, SessionMeta } from '../ports';

/** Maps connectionId -> live browser socket. The local emulator populates this;
 *  LocalPublisher delivers pushes through it. Unused in aws mode (the Management
 *  API reaches the real sockets). */
export const LocalSocketRegistry = new Map<string, { send: (data: string) => void }>();

export class InMemoryConnectionStore implements ConnectionStore {
  private readonly conns = new Map<string, SessionMeta>();
  private readonly rooms = new Map<string, Set<string>>();

  async add(id: string, meta: SessionMeta) {
    this.conns.set(id, meta);
  }
  async remove(id: string) {
    this.conns.delete(id);
    for (const set of this.rooms.values()) set.delete(id);
  }
  async join(id: string, room: string) {
    (this.rooms.get(room) ?? this.rooms.set(room, new Set()).get(room)!).add(id);
  }
  async leave(id: string, room: string) {
    this.rooms.get(room)?.delete(id);
  }
  async membersOf(room: string) {
    return [...(this.rooms.get(room) ?? [])];
  }
}

export class LocalPublisher implements RealtimePublisher {
  constructor(private readonly store: ConnectionStore) {}

  async toConnection(id: string, event: string, data: unknown) {
    const frame = JSON.stringify({ event, data });
    const socket = LocalSocketRegistry.get(id);
    if (socket) {
      socket.send(frame); // deliver to the live browser WebSocket
    } else {
      // No socket registered (e.g. pure curl test) — make the push observable.
      // eslint-disable-next-line no-console
      console.log(`[push -> ${id}] ${event}`, JSON.stringify(data));
    }
  }
  async toRoom(room: string, event: string, data: unknown) {
    const ids = await this.store.membersOf(room);
    await Promise.all(ids.map((id) => this.toConnection(id, event, data)));
  }
}
