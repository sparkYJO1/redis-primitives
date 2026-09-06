import type { Redis } from "ioredis";
import { defineScript, type Script } from "./script.js";

/**
 * Proof that this process held `name` at some point in the past.
 *
 * `token` is strictly increasing across every acquisition of `name`, ever.
 * It is the whole point of this module: a lease can expire without the holder
 * noticing, so the holder's *identity* has to travel with every write it makes
 * and the downstream resource has to reject stale identities. See FencedStore.
 */
export interface Lease {
  readonly name: string;
  readonly token: number;
  /** TTL requested at acquire time. Not a promise that the lease is still live. */
  readonly ttlMs: number;
  /** Local clock reading at acquire time. Only useful for logging. */
  readonly acquiredAt: number;
}

export interface RedisLockOptions {
  /** Namespace for both the lock key and the fencing counter. */
  keyPrefix?: string;
}

// Acquire. Runs as one atomic unit, so nothing can slip between the EXISTS
// check and the SET.
//
// The token is also the lock's value, which means it doubles as the ownership
// proof used by release/extend. INCR never returns the same value twice for a
// live counter, so one integer is enough for both jobs; a separate random
// nonce would add a second thing to get wrong for no extra guarantee.
//
// Returns 0 (never a valid token, since INCR starts at 1) when the lock is held.
const ACQUIRE_LUA = `
if redis.call('EXISTS', KEYS[1]) == 1 then
  return 0
end
local token = redis.call('INCR', KEYS[2])
redis.call('SET', KEYS[1], token, 'PX', ARGV[1])
return token
`;

// Release. Compare-and-delete, never a bare DEL.
//
// A bare DEL says "delete whatever is in this slot". If our lease already
// expired and someone else acquired, a bare DEL deletes *their* lock and the
// system loses mutual exclusion at the exact moment it needs it most.
const RELEASE_LUA = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
`;

// Extend. Same compare-and-swap discipline: only the current owner may push
// the expiry out. Extending a lock you no longer own would steal it.
const EXTEND_LUA = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 0
`;

/**
 * Mutual exclusion on a single Redis instance, with a fencing token.
 *
 * What it guarantees: at most one live lease per name, as long as the Redis
 * instance is up and its clock advances. What it does not guarantee is in the
 * README under "What this does not guarantee" - read that before using it.
 */
export class RedisLock {
  private readonly prefix: string;
  private readonly acquireScript: Script<number>;
  private readonly releaseScript: Script<number>;
  private readonly extendScript: Script<number>;

  constructor(
    private readonly redis: Redis,
    options: RedisLockOptions = {},
  ) {
    this.prefix = options.keyPrefix ?? "lock";
    this.acquireScript = defineScript(redis, "rpLockAcquire", 2, ACQUIRE_LUA);
    this.releaseScript = defineScript(redis, "rpLockRelease", 1, RELEASE_LUA);
    this.extendScript = defineScript(redis, "rpLockExtend", 1, EXTEND_LUA);
  }

  /** Key holding the current owner's token. */
  lockKey(name: string): string {
    return `${this.prefix}:${name}`;
  }

  /**
   * Key holding the monotonic fencing counter.
   *
   * Deliberately has no TTL. If this key is ever lost - evicted under
   * maxmemory, dropped by a restart without persistence, or manually deleted -
   * INCR restarts at 1 and previously issued tokens become reusable. Every
   * safety claim in this repo depends on that not happening.
   */
  fenceKey(name: string): string {
    return `${this.prefix}:${name}:fence`;
  }

  /**
   * One attempt, no retry loop. Returns null if someone else holds the lock.
   *
   * Backoff policy belongs to the caller: how long to wait, and whether to
   * wait at all, depends on the work being protected.
   */
  async acquire(name: string, ttlMs: number): Promise<Lease | null> {
    if (!Number.isInteger(ttlMs) || ttlMs <= 0) {
      throw new Error(`ttlMs must be a positive integer, got ${ttlMs}`);
    }
    const token = await this.acquireScript(
      [this.lockKey(name), this.fenceKey(name)],
      [ttlMs],
    );
    if (token === 0) return null;
    return { name, token, ttlMs, acquiredAt: Date.now() };
  }

  /**
   * Release only if we are still the owner.
   *
   * `false` means the lease had already expired and (usually) someone else has
   * it. That is not an error to swallow silently - it means the critical
   * section ran longer than its lease and any write it made was unprotected.
   */
  async release(lease: Lease): Promise<boolean> {
    const deleted = await this.releaseScript(
      [this.lockKey(lease.name)],
      [String(lease.token)],
    );
    return deleted === 1;
  }

  /**
   * Push the expiry out. `false` means the lease is already gone; the caller
   * must stop touching the protected resource, not retry the extend.
   */
  async extend(lease: Lease, ttlMs: number): Promise<boolean> {
    if (!Number.isInteger(ttlMs) || ttlMs <= 0) {
      throw new Error(`ttlMs must be a positive integer, got ${ttlMs}`);
    }
    const ok = await this.extendScript(
      [this.lockKey(lease.name)],
      [String(lease.token), ttlMs],
    );
    return ok === 1;
  }

  /** Milliseconds left on the lock, or null if it is not held. */
  async remainingTtlMs(name: string): Promise<number | null> {
    const pttl = await this.redis.pttl(this.lockKey(name));
    return pttl >= 0 ? pttl : null;
  }

  /** Current holder's token, or null. Diagnostics only. */
  async currentToken(name: string): Promise<number | null> {
    const value = await this.redis.get(this.lockKey(name));
    return value === null ? null : Number(value);
  }

  /**
   * Run `fn` under the lock, releasing in a finally.
   *
   * The lease is passed in because the token has to reach every write `fn`
   * makes. A withLock that hides the token is a withLock that quietly drops
   * the only defence against an expired lease.
   *
   * Returns null without running `fn` if the lock could not be taken. It does
   * not extend the lease for you: if `fn` can outlive `ttlMs`, either run a
   * heartbeat calling `extend`, or rely on the fencing token downstream, or
   * both.
   */
  async withLock<T>(
    name: string,
    ttlMs: number,
    fn: (lease: Lease) => Promise<T>,
  ): Promise<{ ran: true; value: T } | { ran: false; value: null }> {
    const lease = await this.acquire(name, ttlMs);
    if (!lease) return { ran: false, value: null };
    try {
      return { ran: true, value: await fn(lease) };
    } finally {
      // Safe even if the lease expired mid-flight: release is a
      // compare-and-delete, so an expired holder deletes nothing.
      await this.release(lease);
    }
  }
}
