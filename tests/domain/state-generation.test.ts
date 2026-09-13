import Database from 'better-sqlite3';
import { spawn } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { createContext, type AppContext } from '../../src/context.js';

interface VersionedState {
  generation: number;
  present: boolean;
  entry: {
    namespace: string;
    key: string;
    value: string;
    updated_by: string;
    updated_at: string;
    expires_at?: string | null;
  } | null;
}

interface TransitionResult {
  swapped: boolean;
  predecessor: VersionedState;
  successor: VersionedState;
}

const ABSENT_ZERO: VersionedState = { generation: 0, present: false, entry: null };

function expectVersion(
  state: VersionedState,
  generation: number,
  expected: { value: string; updatedBy: string } | null,
): void {
  expect(Object.keys(state).sort()).toEqual(['entry', 'generation', 'present']);
  expect(state.generation).toBe(generation);
  expect(state.present).toBe(expected !== null);
  if (!expected) {
    expect(state).toEqual({ generation, present: false, entry: null });
    return;
  }
  expect(state.entry).not.toBeNull();
  expect(Object.keys(state.entry!).sort()).toEqual([
    'expires_at',
    'key',
    'namespace',
    'updated_at',
    'updated_by',
    'value',
  ]);
  expect(state.entry).toMatchObject({ value: expected.value, updated_by: expected.updatedBy });
}

function createLegacyV6Database(path: string): void {
  const db = new Database(path);
  db.exec(`
    CREATE TABLE _meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
    INSERT INTO _meta (key, value) VALUES ('schema_version', '6');
    CREATE TABLE agents (
      id TEXT PRIMARY KEY,
      status TEXT NOT NULL,
      last_heartbeat TEXT NOT NULL
    );
    CREATE TABLE channels (id TEXT PRIMARY KEY, archived_at TEXT);
    CREATE TABLE channel_members (channel_id TEXT, agent_id TEXT);
    CREATE TABLE messages (id INTEGER PRIMARY KEY, created_at TEXT NOT NULL);
    CREATE TABLE message_reads (message_id INTEGER NOT NULL);
    CREATE TABLE feed_events (id INTEGER PRIMARY KEY, created_at TEXT NOT NULL);
    CREATE TABLE state (
      namespace TEXT NOT NULL DEFAULT 'default',
      key TEXT NOT NULL,
      value TEXT NOT NULL,
      updated_by TEXT NOT NULL,
      updated_at TEXT NOT NULL DEFAULT (datetime('now')),
      expires_at TEXT,
      PRIMARY KEY (namespace, key)
    );
    CREATE INDEX idx_state_namespace ON state(namespace);
    CREATE INDEX idx_state_expires ON state(expires_at) WHERE expires_at IS NOT NULL;
    INSERT INTO state (namespace, key, value, updated_by)
    VALUES ('legacy', 'key', 'preserved', 'legacy-owner');
  `);
  db.close();
}

function runStateWorker<T>(path: string, owner: string, mode = 'claim'): Promise<T> {
  return new Promise((resolve, reject) => {
    const executable = join(process.cwd(), 'node_modules', '.bin', 'tsx');
    const fixture = fileURLToPath(new URL('../fixtures/generation-claimer.ts', import.meta.url));
    const child = spawn(executable, [fixture, path, owner, mode], {
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    const stdout: Buffer[] = [];
    const stderr: Buffer[] = [];
    child.stdout.on('data', (chunk: Buffer) => stdout.push(chunk));
    child.stderr.on('data', (chunk: Buffer) => stderr.push(chunk));
    child.once('error', reject);
    child.once('exit', (code) => {
      if (code !== 0) {
        reject(new Error(Buffer.concat(stderr).toString() || `generation claimer exited ${code}`));
        return;
      }
      resolve(JSON.parse(Buffer.concat(stdout).toString()) as T);
    });
  });
}

describe('StateService generation and tombstones', () => {
  let ctx: AppContext;
  let ownerA: string;
  let ownerB: string;

  beforeEach(() => {
    ctx = createContext({ path: ':memory:' });
    ownerA = ctx.agents.register({ name: 'generation-a' }).id;
    ownerB = ctx.agents.register({ name: 'generation-b' }).id;
  });

  afterEach(() => ctx.close());

  it('defines a never-seen key as absent generation zero and claims generation one', () => {
    expect(ctx.state.getVersioned('leases', 'camera')).toEqual(ABSENT_ZERO);

    const result = ctx.state.compareGeneration('leases', 'camera', 0, {
      type: 'set',
      value: 'owner-a',
      updatedBy: ownerA,
    }) as TransitionResult;

    expect(result.swapped).toBe(true);
    expect(result.predecessor).toEqual(ABSENT_ZERO);
    expectVersion(result.successor, 1, { value: 'owner-a', updatedBy: ownerA });
  });

  it('prevents set-delete-same-value ABA across a durable tombstone', () => {
    const created = ctx.state.compareGeneration('leases', 'camera', 0, {
      type: 'set',
      value: 'same-raw',
      updatedBy: ownerA,
    }) as TransitionResult;
    const deleted = ctx.state.compareGeneration('leases', 'camera', 1, {
      type: 'delete',
    }) as TransitionResult;

    expectVersion(created.successor, 1, { value: 'same-raw', updatedBy: ownerA });
    expectVersion(deleted.predecessor, 1, { value: 'same-raw', updatedBy: ownerA });
    expect(deleted.successor).toEqual({ generation: 2, present: false, entry: null });
    expect(deleted.swapped).toBe(true);
    expect(
      ctx.db.queryOne<{
        generation: number;
        present: number;
        value: string;
        updated_by: string;
        expires_at: string | null;
      }>(
        `SELECT generation, present, value, updated_by, expires_at
         FROM state WHERE namespace = 'leases' AND key = 'camera'`,
      ),
    ).toEqual({ generation: 2, present: 0, value: '', updated_by: '', expires_at: null });

    const recreated = ctx.state.compareGeneration('leases', 'camera', 2, {
      type: 'set',
      value: 'same-raw',
      updatedBy: ownerA,
    }) as TransitionResult;
    expectVersion(recreated.predecessor, 2, null);
    expectVersion(recreated.successor, 3, { value: 'same-raw', updatedBy: ownerA });

    const stale = ctx.state.compareGeneration('leases', 'camera', 1, {
      type: 'delete',
    }) as TransitionResult;
    expect(stale).toEqual({
      swapped: false,
      predecessor: recreated.successor,
      successor: recreated.successor,
    });
    expectVersion(ctx.state.getVersioned('leases', 'camera'), 3, {
      value: 'same-raw',
      updatedBy: ownerA,
    });
  });

  it('leaves a same-value foreign owner untouched on generation mismatch', () => {
    ctx.state.set('leases', 'camera', 'same-raw', ownerA);
    ctx.state.set('leases', 'camera', 'same-raw', ownerB);
    const foreign = ctx.state.getVersioned('leases', 'camera') as VersionedState;

    const stale = ctx.state.compareGeneration('leases', 'camera', 1, {
      type: 'set',
      value: 'same-raw',
      updatedBy: ownerA,
    }) as TransitionResult;

    expectVersion(foreign, 2, { value: 'same-raw', updatedBy: ownerB });
    expect(stale).toEqual({ swapped: false, predecessor: foreign, successor: foreign });
    expect(ctx.state.getVersioned('leases', 'camera')).toEqual(foreign);
  });

  it('refreshes the same value as one new generation', () => {
    ctx.state.set('leases', 'camera', 'owner-a', ownerA);
    const refreshed = ctx.state.compareGeneration('leases', 'camera', 1, {
      type: 'set',
      value: 'owner-a',
      updatedBy: ownerA,
      ttlSeconds: 60,
    }) as TransitionResult;

    expectVersion(refreshed.predecessor, 1, { value: 'owner-a', updatedBy: ownerA });
    expectVersion(refreshed.successor, 2, { value: 'owner-a', updatedBy: ownerA });
    expect(refreshed.successor.entry?.expires_at).toBeTruthy();
  });

  it('advances through legacy set, value-CAS, delete, and recreate', () => {
    ctx.state.set('legacy', 'key', 'one', ownerA);
    expectVersion(ctx.state.getVersioned('legacy', 'key'), 1, {
      value: 'one',
      updatedBy: ownerA,
    });

    expect(ctx.state.compareAndSwap('legacy', 'key', 'one', 'two', ownerB)).toBe(true);
    expectVersion(ctx.state.getVersioned('legacy', 'key'), 2, {
      value: 'two',
      updatedBy: ownerB,
    });

    expect(ctx.state.delete('legacy', 'key')).toBe(true);
    expect(ctx.state.get('legacy', 'key')).toBeNull();
    expect(ctx.state.getVersioned('legacy', 'key')).toEqual({
      generation: 3,
      present: false,
      entry: null,
    });

    expect(ctx.state.compareAndSwap('legacy', 'key', null, 'three', ownerA)).toBe(true);
    expectVersion(ctx.state.getVersioned('legacy', 'key'), 4, {
      value: 'three',
      updatedBy: ownerA,
    });
  });

  it('linearizes TTL expiry and recreation as separate generations', () => {
    ctx.state.set('ttl', 'key', 'old', ownerA, 60);
    ctx.db.run(
      `UPDATE state SET expires_at = datetime('now', '-1 second') WHERE namespace = ? AND key = ?`,
      ['ttl', 'key'],
    );

    expect(ctx.state.getVersioned('ttl', 'key')).toEqual({
      generation: 2,
      present: false,
      entry: null,
    });
    const recreated = ctx.state.compareGeneration('ttl', 'key', 2, {
      type: 'set',
      value: 'new',
      updatedBy: ownerB,
    }) as TransitionResult;
    expectVersion(recreated.predecessor, 2, null);
    expectVersion(recreated.successor, 3, { value: 'new', updatedBy: ownerB });
  });

  it('turns retention cleanup, stale-owner purge, full purge, and namespace delete into tombstones', () => {
    ctx.state.set('old', 'retention', 'a', ownerA);
    ctx.db.run(
      `UPDATE state SET updated_at = datetime('now', '-30 days') WHERE namespace = 'old' AND key = 'retention'`,
    );
    expect(ctx.cleanup.run().state).toBe(1);
    expect(ctx.state.getVersioned('old', 'retention')).toEqual({
      generation: 2,
      present: false,
      entry: null,
    });

    ctx.state.set('old', 'stale-owner', 'b', ownerA);
    ctx.agents.unregister(ownerA);
    ctx.db.run(`UPDATE agents SET last_heartbeat = datetime('now', '-2 hours') WHERE id = ?`, [
      ownerA,
    ]);
    expect(ctx.cleanup.purgeStaleAssociated().state).toBe(1);
    expect(ctx.state.getVersioned('old', 'stale-owner')).toEqual({
      generation: 2,
      present: false,
      entry: null,
    });

    ctx.state.set('full', 'key', 'c', ownerB);
    expect(ctx.cleanup.purgeEverything().state).toBeGreaterThanOrEqual(1);
    expect(ctx.state.getVersioned('full', 'key')).toEqual({
      generation: 2,
      present: false,
      entry: null,
    });

    ctx.state.set('namespace', 'a', '1', ownerB);
    ctx.state.set('namespace', 'b', '2', ownerB);
    expect(ctx.state.deleteNamespace('namespace')).toBe(2);
    expect(ctx.state.getVersioned('namespace', 'a')).toEqual({
      generation: 2,
      present: false,
      entry: null,
    });
    expect(ctx.state.getVersioned('namespace', 'b')).toEqual({
      generation: 2,
      present: false,
      entry: null,
    });
  });

  it('fails closed at generation exhaustion and rejects invalid expected generations', () => {
    ctx.state.set('limits', 'key', 'held', ownerA);
    ctx.db.run(`UPDATE state SET generation = ? WHERE namespace = 'limits' AND key = 'key'`, [
      Number.MAX_SAFE_INTEGER,
    ]);

    expect(() =>
      ctx.state.compareGeneration('limits', 'key', Number.MAX_SAFE_INTEGER, {
        type: 'delete',
      }),
    ).toThrow('exhausted');
    expectVersion(ctx.state.getVersioned('limits', 'key'), Number.MAX_SAFE_INTEGER, {
      value: 'held',
      updatedBy: ownerA,
    });

    for (const invalid of [-1, 1.5, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1]) {
      expect(() =>
        ctx.state.compareGeneration('limits', 'other', invalid, {
          type: 'set',
          value: 'x',
          updatedBy: ownerA,
        }),
      ).toThrow('safe integer');
    }
    expect(ctx.state.getVersioned('limits', 'other')).toEqual(ABSENT_ZERO);
    expect(() =>
      ctx.db.run(`UPDATE state SET generation = ? WHERE namespace = 'limits' AND key = 'key'`, [
        Number.MAX_SAFE_INTEGER + 1,
      ]),
    ).toThrow();
  });
});

describe('state generation durability and migration', () => {
  it('atomically migrates a v6 live row to generation one and remains idempotent', () => {
    const dir = mkdtempSync(join(tmpdir(), 'agent-comm-v6-'));
    const path = join(dir, 'legacy.db');
    try {
      createLegacyV6Database(path);
      for (let open = 0; open < 2; open++) {
        const ctx = createContext({ path });
        expectVersion(ctx.state.getVersioned('legacy', 'key'), 1, {
          value: 'preserved',
          updatedBy: 'legacy-owner',
        });
        expect(
          ctx.db.queryOne<{ value: string }>(
            `SELECT value FROM _meta WHERE key = 'schema_version'`,
          ),
        ).toEqual({ value: '7' });
        ctx.close();
      }
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it('persists tombstone generation across a server-context restart', () => {
    const dir = mkdtempSync(join(tmpdir(), 'agent-comm-restart-'));
    const path = join(dir, 'state.db');
    try {
      const first = createContext({ path });
      first.state.set('restart', 'key', 'value', 'owner');
      first.state.delete('restart', 'key');
      first.close();

      const second = createContext({ path });
      expect(second.state.getVersioned('restart', 'key')).toEqual({
        generation: 2,
        present: false,
        entry: null,
      });
      const recreated = second.state.compareGeneration('restart', 'key', 2, {
        type: 'set',
        value: 'value',
        updatedBy: 'owner',
      }) as TransitionResult;
      expectVersion(recreated.successor, 3, { value: 'value', updatedBy: 'owner' });
      second.close();
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it('admits exactly one of sixteen concurrent generation-zero claimers', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'agent-comm-race-'));
    const path = join(dir, 'state.db');
    try {
      const initial = createContext({ path });
      initial.close();

      const results = await Promise.all(
        Array.from({ length: 16 }, (_, index) =>
          runStateWorker<TransitionResult>(path, `owner-${index}`),
        ),
      );
      expect(results.filter((result) => result.swapped)).toHaveLength(1);
      const winnerOwner = `owner-${results.findIndex((result) => result.swapped)}`;
      for (const result of results) {
        if (result.swapped) {
          expect(result.predecessor).toEqual(ABSENT_ZERO);
          expectVersion(result.successor, 1, { value: 'claimed', updatedBy: winnerOwner });
        } else {
          expectVersion(result.predecessor, 1, { value: 'claimed', updatedBy: winnerOwner });
          expect(result.successor).toEqual(result.predecessor);
        }
      }

      const final = createContext({ path });
      expectVersion(final.state.getVersioned('race', 'claim'), 1, {
        value: 'claimed',
        updatedBy: winnerOwner,
      });
      final.close();
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  }, 20_000);

  it('linearizes concurrent set/delete and repeated expiry races once each', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'agent-comm-transition-race-'));
    const path = join(dir, 'state.db');
    try {
      const initial = createContext({ path });
      initial.state.set('race', 'transition', 'initial', 'initial-owner');
      initial.state.set('race', 'expiry', 'expiring', 'initial-owner', 60);
      initial.db.run(
        `UPDATE state SET expires_at = datetime('now', '-1 second')
         WHERE namespace = 'race' AND key = 'expiry'`,
      );
      initial.close();

      const transitions = await Promise.all(
        Array.from({ length: 16 }, (_, index) =>
          runStateWorker<TransitionResult>(
            path,
            `racer-${index}`,
            index % 2 === 0 ? 'set' : 'delete',
          ),
        ),
      );
      const winners = transitions.filter((result) => result.swapped);
      expect(winners).toHaveLength(1);
      expect(winners[0].predecessor.generation).toBe(1);
      expect(winners[0].successor.generation).toBe(2);
      for (const loser of transitions.filter((result) => !result.swapped)) {
        expect(loser.predecessor.generation).toBe(2);
        expect(loser.successor).toEqual(loser.predecessor);
      }

      const expiredReads = await Promise.all(
        Array.from({ length: 16 }, (_, index) =>
          runStateWorker<VersionedState>(path, `reader-${index}`, 'get'),
        ),
      );
      expect(new Set(expiredReads.map((state) => state.generation))).toEqual(new Set([2]));
      expect(expiredReads.every((state) => !state.present && state.entry === null)).toBe(true);

      const final = createContext({ path });
      expect(final.state.getVersioned('race', 'transition').generation).toBe(2);
      expect(final.state.getVersioned('race', 'expiry')).toEqual({
        generation: 2,
        present: false,
        entry: null,
      });
      final.close();
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  }, 20_000);
});
