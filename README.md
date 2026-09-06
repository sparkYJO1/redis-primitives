# redis-primitives

A distributed lock with fencing tokens, a sliding-window rate limiter, and
leader election. TypeScript, ioredis, no framework, one Redis instance.

Dozens of libraries already do this. What is different here is the test suite:
**every primitive is tested by reproducing the failure it exists to prevent**,
against real Redis, with no mocks. A lock that locks proves nothing. A lock
whose lease expires mid-critical-section, while the holder carries on writing,
proves something — and it is the only case worth thinking about.

```bash
npm install
npm run redis:up      # Redis 7.4 on port 56380
npm test              # 27 tests, ~10s, all against the real server
npm run redis:down
```

---

## 1. The lock, and why a lock is not enough

### The failure

```ts
// Looks correct. Is not.
const ok = await redis.set("lock:account-42", "1", "NX", "PX", 10_000);
if (ok) {
  try {
    await chargeAccount(42);          // takes 12s once a week
  } finally {
    await redis.del("lock:account-42");  // <-- two separate bugs
  }
}
```

Two things go wrong, and they go wrong together:

1. **The lease expires while the work is still running.** At 10s the key is
   gone; at 10.1s another worker acquires it; from 10.1s to 12s two workers are
   inside a section that admits one. Nothing warns the first worker. Nothing
   can — a Redis key cannot reach into a process and stop it.
2. **`DEL` deletes whoever's lock is there now.** By the time the first worker
   reaches its `finally`, the key belongs to the second worker. The first
   worker deletes it, and a third worker walks in.

### The fix

```ts
const lock = new RedisLock(redis);
const lease = await lock.acquire("account-42", 10_000);
if (lease) {
  try {
    await chargeAccount(42, lease.token);   // the token travels with the write
  } finally {
    await lock.release(lease);              // compare-and-delete, never bare DEL
  }
}
```

`release` is a Lua compare-and-delete: it deletes the key only if the key still
holds *our* token. An expired holder's cleanup deletes nothing.

`acquire` returns a **fencing token** — an integer from `INCR`, strictly
increasing across every acquisition of that name, ever. It is the answer to
problem 1, and it works by giving up on solving it at the lock:

> The overlap cannot be prevented. Two processes will sometimes both believe
> they hold the lock, because that belief lives in process memory. What can be
> prevented is the stale process's *writes* landing after the new holder's —
> and the only place that can be enforced is the resource being written to.

So the resource has to check:

```ts
// FencedStore, or in Postgres:
//   UPDATE account SET balance = $1, fence_token = $2
//   WHERE id = $3 AND fence_token <= $2
//   -- rowCount === 0 means you were fenced out
const result = await store.write("account-42", newBalance, lease.token);
if (!result.accepted) {
  // Our lease expired and someone newer has already written. Stop.
}
```

### The tests

`tests/fencing.test.ts` runs exactly that scenario: a 200ms lease, 350ms of
work, a second holder acquiring at 220ms.

| test | shows |
|---|---|
| `lets a second holder in, and the fenced resource rejects the first one's write` | the stale write is refused; the new holder's value survives |
| `without fencing, the same sequence silently clobbers the new holder's write` | the identical sequence against a plain `SET` — the stale write **wins** |
| `does NOT reject a stale writer that gets there first` | the honest limit: fencing orders writes, it does not detect staleness |
| `loses every guarantee if the fencing counter is lost` | delete the counter key and the same token is issued twice |

`tests/lock.test.ts` covers mutual exclusion under real concurrency: 40
concurrent `acquire` calls on one shared connection, 15 rounds, exactly one
winner every round, tokens strictly increasing across all of them.

---

## 2. The rate limiter

### The failure

```ts
// The first result for "rate limiting with Redis".
const bucket = Math.floor(Date.now() / 60_000);
const count = await redis.incr(`rl:${userId}:${bucket}`);
await redis.expire(`rl:${userId}:${bucket}`, 60);
return count <= 100;
```

The counter resets at a wall-clock instant rather than relative to traffic, so
a caller spends its whole allowance at the end of one bucket and its whole
allowance at the start of the next:

```
                      bucket N          |  bucket N+1
  requests  . . . . . . . . . . . ##### | ##### . . . . . . . . . .
                                        ^ counter resets here
            |<-------------------- 104ms -------------------->|
                       200 requests against a limit of 100
```

`tests/rate-limiter.test.ts` fires 5 requests at the end of one window and 5 at
the start of the next, with the clock pinned. All 10 are admitted, inside a
span of 104ms, against a limit of 5 per 1000ms.

Separately: `INCR` and `EXPIRE` are two round trips. A crash between them
leaves a counter with no TTL, forever. Asserted, not asserted-about.

### The second failure

The usual next attempt fixes the algorithm and not the atomicity:

```ts
await redis.zremrangebyscore(key, "-inf", now - windowMs);
const used = await redis.zcard(key);
if (used >= limit) return false;
await redis.zadd(key, now, randomUUID());   // <-- three round trips
```

Correct algorithm. Under concurrency every caller reads `ZCARD` before any
caller has run `ZADD`, so every caller sees room. Measured, 100 concurrent
callers against a limit of 5:

```
naive client-side sliding window: 100/100 admitted against a limit of 5
```

Not "occasionally overshoots". It admitted everything.

### The fix

One Lua script: trim, count, decide, record, all server-side, with nothing
interleaved. Same traffic, same test file:

```
the Lua sliding window admits exactly the limit:  50/200 admitted, limit 50
```

The window clock is Redis' own (`TIME` inside the script), not the caller's, so
skew between app servers cannot widen or narrow the window. Tests pin the clock
explicitly; production never does.

`WATCH`/`MULTI` was the first implementation and it failed in a way worth
reading about — see [ADR 0002](docs/decisions/0002-lua-over-watch-multi.md).

---

## 3. Leader election

There is no separate algorithm here: leadership is a lock that renews itself,
and the epoch is the lock's fencing token. Naming it "election" does not buy a
guarantee the lock did not already have.

```ts
const election = new LeaderElection(lock, {
  name: "cron-scheduler",
  nodeId: process.env.HOSTNAME!,
  leaseTtlMs: 10_000,
  renewIntervalMs: 3_000,
  onElected: (epoch) => log.info({ epoch }, "elected"),
  onDemoted: (reason, epoch) => log.warn({ reason, epoch }, "stepped down"),
});
election.start();

// Every side effect carries the epoch. Without this, election is decoration.
if (election.isLeader) await store.write("schedule", payload, election.epoch!);
```

If Redis is unreachable during a renewal the node **steps down**, because it
cannot prove it still holds the lease, and acting as leader on an unverifiable
lease is the exact failure the design is trying to bound.

### The split-brain window, measured rather than hand-waved

The intuition is that two leaders can overlap for at most one lease TTL. That
is wrong, and `tests/leader-election.test.ts` measures how wrong:

```
clean handover took 27ms                                   (leader calls stop(), releases)
failover took 403ms (lease 400ms)                          (leader stalls, lease expires)
split-brain window: 1095ms (lease TTL 400ms, stall 1500ms) (both nodes believe they lead)
```

The window is bounded by **how long the old leader's process was unable to
check**, not by the TTL:

```
  lower bound before failover can begin:  leaseTtlMs - renewIntervalMs
  old leader stays unaware for:           stall duration + renewIntervalMs + one RTT
```

A process paused long enough — GC, a suspended VM, a CPU-throttled container —
stays unaware indefinitely. Nothing in Redis bounds that. During the measured
1095ms the test asserts that **both nodes report `isLeader === true`**, that the
new leader's write is accepted, and that the old leader's write with the stale
epoch is rejected. That rejection is the only thing standing between the two of
them, which is why `epoch` is not optional.

---

## What this does not guarantee

Read this section before using any of it.

**It is not consensus, and it is not Redlock.** One Redis instance with a TTL
is a lease, not a quorum. There is no majority, no quorum read, no partition
tolerance. If you need consensus, use etcd, ZooKeeper or Consul.
[ADR 0003](docs/decisions/0003-single-instance-not-redlock.md) explains why
adding Redlock would not have fixed the thing this repo is about.

**A fencing token you do not check is worse than no fencing token,** because it
looks like protection. `acquire` returning a token protects nothing on its own.
The resource must implement the comparison. Resources that cannot express it —
a plain S3 PUT, an append-only log, a webhook to a third party — cannot be
fenced at all; make those operations idempotent instead.

**Fencing orders writes; it does not detect staleness.** If the expired holder
writes *before* the new holder writes anything, the resource has never seen a
higher token and accepts it. What is guaranteed is that once a newer write
lands, no older holder can overwrite it. Asserted in `tests/fencing.test.ts`.

**Single instance means a single point of failure.** If Redis is down,
`acquire` throws and nothing is locked. If Redis fails over to a replica,
replication is asynchronous: a lock acknowledged by the old master may not
exist on the new one, and the fencing counter can go backwards — which reissues
tokens and breaks the ordering property everything else rests on. This design
does not survive a failover; it is only honest about it.

**The fencing counter must never be lost.** It has no TTL by design. If it is
evicted under `maxmemory`, flushed, or lost to a restart without persistence,
`INCR` restarts at 1 and previously issued tokens become reusable. Run with
`maxmemory-policy noeviction`, as `docker-compose.yml` does. There is a test
that deletes the counter and shows the same token being handed to two holders.

**Expiry follows Redis' clock, not yours.** Lock TTLs and the rate limiter
window both come from the server, which removes app-server skew as a variable
but makes the Redis host's clock load-bearing. An NTP step backwards on that
host extends every live lease; a step forwards expires them early. The rate
limiter never reads a client clock in production for this reason.

**`acquire` does not retry.** One attempt, `null` if taken. Backoff policy is
the caller's, because how long to wait depends on the work being protected.

**The sliding-window log is O(limit) memory per key.** One ZSET member per
allowed request. Fine for limits in the hundreds; for "10,000 per hour per
tenant" use a sliding-window *counter* (two weighted buckets) and accept the
approximation.

**Redis is single-threaded, and Lua scripts block it.** Every script here is
O(log N) plus a bounded trim. A slow script would stall the whole server.

---

## Layout

```
src/lock.ts             mutual exclusion, compare-and-delete release, fencing token
src/fenced-store.ts     the other half of fencing: a resource that refuses stale writers
src/rate-limiter.ts     sliding window log, one Lua script
src/leader-election.ts  a lock that renews itself; epoch == fencing token
src/naive.ts            the broken versions, kept so the tests can fail against them
src/script.ts           EVALSHA-backed script loading

tests/lock.test.ts             mutual exclusion, release discipline, extend, withLock
tests/fencing.test.ts          the expired-lease scenario, and what fencing does not fix
tests/rate-limiter.test.ts     boundary doubling, atomicity under 100 concurrent callers
tests/leader-election.test.ts  failover timing and the measured split-brain window

experiments/watch-multi-race.ts  the WATCH/MULTI attempt that failed, reproducible
docs/decisions/                  three ADRs
```

Tests run against a real Redis on port 56380 (`docker compose up -d`). There is
no mock layer and there is not going to be one: a mock cannot expire a TTL
underneath a running critical section, cannot interleave 100 callers between a
`ZCARD` and a `ZADD`, and cannot tell you whether a script is atomic. Those are
the only things being tested.

## Decisions

- [0001 — A fencing token, not a longer TTL](docs/decisions/0001-fencing-token-over-a-longer-ttl.md)
- [0002 — Lua scripts, not WATCH/MULTI](docs/decisions/0002-lua-over-watch-multi.md) — includes the attempt that failed, with measurements
- [0003 — Single-instance Redis, and explicitly not Redlock](docs/decisions/0003-single-instance-not-redlock.md)

## How this was built

Built with Claude (Anthropic's Claude Code), against design decisions I made
and reviewed. The scope, the guarantees claimed, the choice to test failure
modes rather than happy paths, and every statement in "What this does not
guarantee" are mine. I have read every line, and I can defend every line.

The WATCH/MULTI numbers in ADR 0002, the split-brain window in the leader
election test, and the 100/100 overshoot figure are all real outputs from this
repo, reproducible with `npm test` and `npm run experiment:watch-multi`.

## Licence

MIT
