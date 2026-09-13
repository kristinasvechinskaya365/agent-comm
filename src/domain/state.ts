// =============================================================================
// agent-comm — Shared state domain
//
// Namespaced key-value store shared across all agents. Supports atomic
// operations, namespace listing, and prefix-filtered queries.
// =============================================================================

import type { Db } from '../storage/database.js';
import type { EventBus } from './events.js';
import type {
  StateEntry,
  StateGenerationTransitionResult,
  StateTransitionIntent,
  StateVersion,
} from '../types.js';
import { ConflictError, ValidationError } from '../types.js';

const MAX_KEY_LENGTH = 256;
const MAX_VALUE_LENGTH = 100_000;
const MAX_DATE_MS = 8_640_000_000_000_000;
export const MAX_STATE_GENERATION = Number.MAX_SAFE_INTEGER;

const STATE_COLUMNS =
  'namespace, key, value, updated_by, updated_at, expires_at, generation, present';

interface StateRow extends StateEntry {
  readonly generation: number;
  readonly present: number;
}

type NormalizedTransition =
  | {
      readonly type: 'set';
      readonly value: string;
      readonly updatedBy: string;
      readonly expiresAt: string | null;
    }
  | { readonly type: 'delete' };

export class StateService {
  constructor(
    private readonly db: Db,
    private readonly events?: EventBus,
  ) {}

  set(
    namespace: string,
    key: string,
    value: string,
    updatedBy: string,
    ttlSeconds?: number,
  ): StateEntry {
    this.validateKey(namespace, key);
    const transition = this.normalizeSet(value, updatedBy, ttlSeconds);
    const successor = this.writeTransaction(() => {
      this.expireMatching(
        `namespace = ? AND key = ? AND expires_at IS NOT NULL
         AND datetime(expires_at) <= datetime('now')`,
        [namespace, key],
      );
      return this.applyTransition(namespace, key, this.readVersion(namespace, key), transition);
    });

    this.emitTransition(namespace, key, successor);
    return successor.entry!;
  }

  get(namespace: string, key: string): StateEntry | null {
    return this.getVersioned(namespace, key).entry;
  }

  /** Return the durable incarnation state, including absent generation zero/tombstones. */
  getVersioned(namespace: string, key: string): StateVersion {
    this.validateKey(namespace, key);
    this.expireSweep();
    return this.readVersion(namespace, key);
  }

  list(namespace?: string, prefix?: string): StateEntry[] {
    if (namespace !== undefined) this.validateNamespace(namespace);
    if (prefix !== undefined) this.validatePrefix(prefix);
    this.expireSweep();

    let sql = `SELECT namespace, key, value, updated_by, updated_at, expires_at
               FROM state WHERE present = 1`;
    const params: unknown[] = [];

    if (namespace) {
      sql += ` AND namespace = ?`;
      params.push(namespace);
    }
    if (prefix) {
      sql += ` AND key LIKE ? ESCAPE '\\'`;
      params.push(prefix.replace(/[\\%_]/g, '\\$&') + '%');
    }

    sql += ` ORDER BY namespace, key`;
    return this.db.queryAll<StateEntry>(sql, params);
  }

  namespaces(): string[] {
    this.expireSweep();
    const rows = this.db.queryAll<{ namespace: string }>(
      `SELECT DISTINCT namespace FROM state WHERE present = 1 ORDER BY namespace`,
    );
    return rows.map((row) => row.namespace);
  }

  delete(namespace: string, key: string): boolean {
    this.validateKey(namespace, key);
    const result = this.writeTransaction(() => {
      this.expireMatching(
        `namespace = ? AND key = ? AND expires_at IS NOT NULL
         AND datetime(expires_at) <= datetime('now')`,
        [namespace, key],
      );
      const current = this.readVersion(namespace, key);
      if (!current.present) return null;
      return this.applyTransition(namespace, key, current, { type: 'delete' });
    });

    if (!result) return false;
    this.emitTransition(namespace, key, result);
    return true;
  }

  deleteNamespace(namespace: string): number {
    this.validateNamespace(namespace);
    const count = this.writeTransaction(() => {
      const expired = this.expireMatching(
        `namespace = ? AND expires_at IS NOT NULL
         AND datetime(expires_at) <= datetime('now')`,
        [namespace],
      );
      return expired + this.tombstoneMatching(`namespace = ?`, [namespace]);
    });

    if (count > 0) this.events?.emit('state:deleted', { namespace });
    return count;
  }

  compareAndSwap(
    namespace: string,
    key: string,
    expected: string | null,
    newValue: string,
    updatedBy: string,
    ttlSeconds?: number,
  ): boolean {
    this.validateKey(namespace, key);
    if (expected !== null && typeof expected !== 'string') {
      throw new ValidationError('expected must be a string or null.');
    }
    const transition: NormalizedTransition =
      newValue === '' ? { type: 'delete' } : this.normalizeSet(newValue, updatedBy, ttlSeconds);

    const successor = this.writeTransaction(() => {
      this.expireMatching(
        `namespace = ? AND key = ? AND expires_at IS NOT NULL
         AND datetime(expires_at) <= datetime('now')`,
        [namespace, key],
      );
      const current = this.readVersion(namespace, key);
      if ((current.entry?.value ?? null) !== expected) return null;
      return this.applyTransition(namespace, key, current, transition);
    });

    if (!successor) return false;
    this.emitTransition(namespace, key, successor);
    return true;
  }

  /** Compare one server-owned generation and apply exactly one set/delete transition. */
  compareGeneration(
    namespace: string,
    key: string,
    expectedGeneration: number,
    intent: StateTransitionIntent,
  ): StateGenerationTransitionResult {
    this.validateKey(namespace, key);
    this.validateExpectedGeneration(expectedGeneration);
    const transition = this.normalizeIntent(intent);

    const result = this.writeTransaction<StateGenerationTransitionResult>(() => {
      this.expireMatching(
        `namespace = ? AND key = ? AND expires_at IS NOT NULL
         AND datetime(expires_at) <= datetime('now')`,
        [namespace, key],
      );
      const predecessor = this.readVersion(namespace, key);
      if (predecessor.generation !== expectedGeneration) {
        return { swapped: false, predecessor, successor: predecessor };
      }
      const successor = this.applyTransition(namespace, key, predecessor, transition);
      return { swapped: true, predecessor, successor };
    });

    if (result.swapped) this.emitTransition(namespace, key, result.successor);
    return result;
  }

  /** Tombstone present state older than the cleanup retention threshold. */
  tombstoneOlderThan(maxAgeDays: number): number {
    if (!Number.isFinite(maxAgeDays) || maxAgeDays <= 0) {
      throw new ValidationError('maxAgeDays must be a positive number.');
    }
    return this.writeTransaction(() =>
      this.tombstoneMatching(`updated_at < datetime('now', ?)`, [`-${maxAgeDays} days`]),
    );
  }

  /** Tombstone present state owned by any of the supplied agent IDs. */
  tombstoneByUpdatedBy(agentIds: readonly string[]): number {
    if (agentIds.length === 0) return 0;
    const placeholders = agentIds.map(() => '?').join(',');
    return this.writeTransaction(() =>
      this.tombstoneMatching(`updated_by IN (${placeholders})`, [...agentIds]),
    );
  }

  /** Tombstone every present state entry while retaining incarnation history. */
  tombstoneAll(): number {
    return this.writeTransaction(() => this.tombstoneMatching('1 = 1', []));
  }

  private expireSweep(): number {
    return this.writeTransaction(() =>
      this.expireMatching(`expires_at IS NOT NULL AND datetime(expires_at) <= datetime('now')`, []),
    );
  }

  private expireMatching(where: string, params: unknown[]): number {
    return this.tombstoneMatching(where, params);
  }

  private tombstoneMatching(where: string, params: unknown[]): number {
    const rows = this.db.queryAll<StateRow>(
      `SELECT ${STATE_COLUMNS} FROM state WHERE present = 1 AND (${where})
       ORDER BY namespace, key`,
      params,
    );
    const versions = rows.map((row) => this.toVersion(row));
    for (const version of versions) this.assertCanAdvance(version.generation);
    for (const version of versions) {
      const entry = version.entry!;
      this.applyTransition(entry.namespace, entry.key, version, { type: 'delete' });
    }
    return versions.length;
  }

  private applyTransition(
    namespace: string,
    key: string,
    current: StateVersion,
    transition: NormalizedTransition,
  ): StateVersion {
    this.assertCanAdvance(current.generation);
    const generation = current.generation + 1;
    // better-sqlite3 binds JavaScript numbers as REAL. The storage schema uses
    // no-affinity columns so it can reject integral REAL inputs without SQLite
    // coercing them; bind server-owned counters as int64 via bigint instead.
    const storedGeneration = BigInt(generation);
    const storedCurrentGeneration = BigInt(current.generation);
    let changes: number;

    if (transition.type === 'set') {
      if (current.generation === 0) {
        changes = this.db.run(
          `INSERT INTO state
             (namespace, key, value, updated_by, expires_at, generation, present)
           VALUES (?, ?, ?, ?, ?, ?, 1)`,
          [
            namespace,
            key,
            transition.value,
            transition.updatedBy,
            transition.expiresAt,
            storedGeneration,
          ],
        ).changes;
      } else {
        changes = this.db.run(
          `UPDATE state
           SET value = ?, updated_by = ?, updated_at = datetime('now'), expires_at = ?,
               generation = ?, present = 1
           WHERE namespace = ? AND key = ? AND generation = ?`,
          [
            transition.value,
            transition.updatedBy,
            transition.expiresAt,
            storedGeneration,
            namespace,
            key,
            storedCurrentGeneration,
          ],
        ).changes;
      }
    } else if (current.generation === 0) {
      changes = this.db.run(
        `INSERT INTO state
           (namespace, key, value, updated_by, expires_at, generation, present)
         VALUES (?, ?, '', '', NULL, ?, 0)`,
        [namespace, key, storedGeneration],
      ).changes;
    } else {
      changes = this.db.run(
        `UPDATE state
         SET value = '', updated_by = '', updated_at = datetime('now'), expires_at = NULL,
             generation = ?, present = 0
         WHERE namespace = ? AND key = ? AND generation = ?`,
        [storedGeneration, namespace, key, storedCurrentGeneration],
      ).changes;
    }

    if (changes !== 1) {
      throw new ConflictError('State changed during transition.');
    }
    return this.readVersion(namespace, key);
  }

  private readVersion(namespace: string, key: string): StateVersion {
    const row = this.db.queryOne<StateRow>(
      `SELECT ${STATE_COLUMNS} FROM state WHERE namespace = ? AND key = ?`,
      [namespace, key],
    );
    return row ? this.toVersion(row) : { generation: 0, present: false, entry: null };
  }

  private toVersion(row: StateRow): StateVersion {
    if (!Number.isSafeInteger(row.generation) || row.generation < 1) {
      throw new ConflictError('Stored state generation is invalid.');
    }
    if (row.present !== 0 && row.present !== 1) {
      throw new ConflictError('Stored state presence is invalid.');
    }
    return {
      generation: row.generation,
      present: row.present === 1,
      entry:
        row.present === 1
          ? {
              namespace: row.namespace,
              key: row.key,
              value: row.value,
              updated_by: row.updated_by,
              updated_at: row.updated_at,
              expires_at: row.expires_at ?? null,
            }
          : null,
    };
  }

  private normalizeIntent(intent: StateTransitionIntent): NormalizedTransition {
    if (typeof intent !== 'object' || intent === null || Array.isArray(intent)) {
      throw new ValidationError('Transition intent must be an object.');
    }
    const record = intent as unknown as Record<string, unknown>;
    const type = record.type;
    if (type === 'delete') {
      this.rejectUnknownFields(record, new Set(['type']));
      return { type: 'delete' };
    }
    if (type === 'set') {
      this.rejectUnknownFields(record, new Set(['type', 'value', 'updatedBy', 'ttlSeconds']));
      return this.normalizeSet(
        record.value as string,
        record.updatedBy as string,
        record.ttlSeconds as number | undefined,
      );
    }
    throw new ValidationError('Transition type must be "set" or "delete".');
  }

  private normalizeSet(
    value: string,
    updatedBy: string,
    ttlSeconds?: number,
  ): NormalizedTransition {
    if (typeof value !== 'string') throw new ValidationError('Value must be a string.');
    if (value.length > MAX_VALUE_LENGTH) {
      throw new ValidationError(`Value exceeds maximum length of ${MAX_VALUE_LENGTH}.`);
    }
    if (typeof updatedBy !== 'string' || !updatedBy.trim()) {
      throw new ValidationError('updatedBy must be a non-empty string.');
    }

    let expiresAt: string | null = null;
    if (ttlSeconds !== undefined) {
      if (typeof ttlSeconds !== 'number' || !Number.isFinite(ttlSeconds) || ttlSeconds <= 0) {
        throw new ValidationError('ttl_seconds must be a positive number.');
      }
      const expiresAtMs = Date.now() + ttlSeconds * 1000;
      if (!Number.isFinite(expiresAtMs) || expiresAtMs > MAX_DATE_MS) {
        throw new ValidationError('ttl_seconds is outside the supported date range.');
      }
      expiresAt = new Date(expiresAtMs).toISOString();
    }
    return { type: 'set', value, updatedBy, expiresAt };
  }

  private validateExpectedGeneration(generation: number): void {
    if (!Number.isSafeInteger(generation) || generation < 0) {
      throw new ValidationError(
        `expected_generation must be a safe integer between 0 and ${MAX_STATE_GENERATION}.`,
      );
    }
  }

  private assertCanAdvance(generation: number): void {
    if (!Number.isSafeInteger(generation) || generation < 0) {
      throw new ConflictError('Stored state generation is invalid.');
    }
    if (generation >= MAX_STATE_GENERATION) {
      throw new ConflictError('State generation exhausted.');
    }
  }

  private rejectUnknownFields(record: Record<string, unknown>, allowed: Set<string>): void {
    const unknown = Object.keys(record).find((field) => !allowed.has(field));
    if (unknown) throw new ValidationError(`Unknown transition field: "${unknown}".`);
  }

  private validateKey(namespace: string, key: string): void {
    this.validateNamespace(namespace);
    if (typeof key !== 'string' || !key || key.length > MAX_KEY_LENGTH) {
      throw new ValidationError(`Key must be 1-${MAX_KEY_LENGTH} characters.`);
    }
    // eslint-disable-next-line no-control-regex
    if (/[\x00-\x1f\x7f]/.test(key)) {
      throw new ValidationError('Key must not contain control characters.');
    }
  }

  private validateNamespace(namespace: string): void {
    if (typeof namespace !== 'string' || !namespace || namespace.length > MAX_KEY_LENGTH) {
      throw new ValidationError(`Namespace must be 1-${MAX_KEY_LENGTH} characters.`);
    }
    // eslint-disable-next-line no-control-regex
    if (/[\x00-\x1f\x7f]/.test(namespace)) {
      throw new ValidationError('Namespace must not contain control characters.');
    }
  }

  private validatePrefix(prefix: string): void {
    if (typeof prefix !== 'string' || prefix.length > MAX_KEY_LENGTH) {
      throw new ValidationError(`Prefix must be at most ${MAX_KEY_LENGTH} characters.`);
    }
    // eslint-disable-next-line no-control-regex
    if (/[\x00-\x1f\x7f]/.test(prefix)) {
      throw new ValidationError('Prefix must not contain control characters.');
    }
  }

  private writeTransaction<T>(fn: () => T): T {
    return this.db.raw.transaction(fn).immediate();
  }

  private emitTransition(namespace: string, key: string, successor: StateVersion): void {
    if (successor.entry) {
      this.events?.emit('state:changed', {
        namespace,
        key,
        value: successor.entry.value,
        updated_by: successor.entry.updated_by,
      });
    } else {
      this.events?.emit('state:deleted', { namespace, key });
    }
  }
}
