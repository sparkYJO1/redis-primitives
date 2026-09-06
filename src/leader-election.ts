import type { Lease, RedisLock } from "./lock.js";

export type DemotionReason =
  /** The lease was gone when we tried to renew it. Someone else may lead now. */
  | "lease-lost"
  /** Redis was unreachable, so we cannot prove we still lead. */
  | "redis-error"
  /** stop() was called. */
  | "stopped";

export interface LeaderElectionOptions {
  /** Contended resource, e.g. "cron-scheduler". */
  name: string;
  /** Human-readable, for logs. Not used for correctness - the epoch is. */
  nodeId: string;
  /** How long a leadership lease lives without renewal. */
  leaseTtlMs: number;
  /** How often the leader renews. Must be well under leaseTtlMs. */
  renewIntervalMs: number;
  /** How often a follower retries. Defaults to renewIntervalMs. */
  campaignIntervalMs?: number;
  onElected?: (epoch: number) => void;
  onDemoted?: (reason: DemotionReason, epoch: number) => void;
  onError?: (error: unknown) => void;
}

/**
 * Leader election as a lock that renews itself.
 *
 * There is no separate algorithm here, and that is the point: leadership is a
 * lease, the epoch is the lease's fencing token, and losing the lease is the
 * same event as a lock expiring. Naming it "election" does not buy any
 * guarantee that the lock did not already have.
 *
 * `epoch` is what Raft calls a term and ZooKeeper calls a zxid-derived epoch.
 * It exists for the same reason: a leader that has been superseded cannot
 * always be told so in time, so every side effect it performs must carry its
 * epoch and be rejected downstream if a higher one has been seen.
 *
 * The split-brain window is real and is not closed by tuning. Between the
 * moment a leader stops renewing and the moment it finds out, a new leader can
 * exist. That window is:
 *
 *   lower bound before failover can start: leaseTtlMs - renewIntervalMs
 *   old leader stays unaware for:          however long its process is stalled,
 *                                          plus renewIntervalMs, plus one RTT
 *
 * A process paused long enough - GC, a suspended VM, a throttled container -
 * stays unaware indefinitely. That is measured in tests/leader-election.test.ts
 * rather than asserted, and the only defence is epoch checking at the resource.
 */
export class LeaderElection {
  private lease: Lease | null = null;
  private running = false;
  private timer: NodeJS.Timeout | null = null;
  private stalledUntilMs = 0;
  private readonly campaignIntervalMs: number;

  constructor(
    private readonly lock: RedisLock,
    private readonly options: LeaderElectionOptions,
  ) {
    if (options.renewIntervalMs >= options.leaseTtlMs) {
      throw new Error(
        "renewIntervalMs must be shorter than leaseTtlMs, otherwise the lease " +
          "expires between renewals by construction",
      );
    }
    this.campaignIntervalMs = options.campaignIntervalMs ?? options.renewIntervalMs;
  }

  get isLeader(): boolean {
    return this.lease !== null;
  }

  /** Current epoch, or null if not leading. Pass this to every write. */
  get epoch(): number | null {
    return this.lease?.token ?? null;
  }

  start(): void {
    if (this.running) return;
    this.running = true;
    this.schedule(0);
  }

  /** Steps down and releases the lease so the next leader does not wait a TTL. */
  async stop(): Promise<void> {
    this.running = false;
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    const lease = this.lease;
    if (lease) {
      this.lease = null;
      try {
        await this.lock.release(lease);
      } catch (error) {
        this.options.onError?.(error);
      }
      this.options.onDemoted?.("stopped", lease.token);
    }
  }

  /**
   * Fault injection for tests. Models a process that is frozen - GC pause,
   * suspended VM, CPU-throttled container - and therefore neither renews its
   * lease nor learns that it lost it, while still believing it leads.
   *
   * Not for production use. There is no legitimate reason to call this.
   */
  simulateProcessStall(durationMs: number): void {
    this.stalledUntilMs = Date.now() + durationMs;
  }

  private schedule(delayMs: number): void {
    if (!this.running) return;
    this.timer = setTimeout(() => void this.tick(), delayMs);
    // Do not hold the event loop open for a background election.
    this.timer.unref?.();
  }

  private async tick(): Promise<void> {
    if (!this.running) return;

    if (Date.now() < this.stalledUntilMs) {
      // Frozen: no renewal, no discovery, still isLeader === true.
      this.schedule(this.options.renewIntervalMs);
      return;
    }

    try {
      if (this.lease) {
        await this.renew(this.lease);
      } else {
        await this.campaign();
      }
    } catch (error) {
      this.options.onError?.(error);
      // Redis is unreachable. We cannot prove the lease is still ours, so we
      // step down. Continuing to act as leader on an unverifiable lease is
      // precisely the split-brain this class exists to bound.
      const lease = this.lease;
      if (lease) {
        this.lease = null;
        this.options.onDemoted?.("redis-error", lease.token);
      }
    }

    this.schedule(
      this.lease ? this.options.renewIntervalMs : this.campaignIntervalMs,
    );
  }

  private async renew(lease: Lease): Promise<void> {
    const stillOurs = await this.lock.extend(lease, this.options.leaseTtlMs);
    if (!stillOurs) {
      this.lease = null;
      this.options.onDemoted?.("lease-lost", lease.token);
    }
  }

  private async campaign(): Promise<void> {
    const lease = await this.lock.acquire(
      this.options.name,
      this.options.leaseTtlMs,
    );
    if (!lease) return;
    this.lease = lease;
    this.options.onElected?.(lease.token);
  }
}
