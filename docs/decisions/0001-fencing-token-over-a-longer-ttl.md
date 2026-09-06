# 1. A fencing token, not a longer TTL

Status: accepted
Date: 2026-09-06

## Context

A lock lease can expire while its holder is still inside the critical section.
The holder does not find out. It has no way to find out synchronously, because
finding out requires a round trip to Redis, and by the time the answer comes
back it is already stale.

The first instinct is to fix this by choosing a better TTL.

## Options considered

**Raise the TTL until overruns stop happening.** This does not work, and it is
worth being precise about why rather than repeating "unbounded pauses" as a
slogan. Two separate objections:

1. It trades a rare correctness failure for a routine availability failure. A
   TTL long enough to cover the p99.9 critical section is also how long every
   other node waits after the holder crashes. At TTL = 60s, one `kill -9`
   stalls the queue for a minute.
2. It does not remove the failure, only its frequency. The pause that has to be
   covered is not "how long does my code take" but "how long can my process be
   unable to run", and a stop-the-world GC, a suspended VM, a throttled
   container, or a network partition can each exceed any constant chosen in
   advance. Picking a TTL is picking a probability, not a guarantee.

**Heartbeat: extend the lease from a timer.** Worth doing, and this repo
implements `extend`, but it is not a solution to the same problem. The
heartbeat runs on the same event loop that the stall froze. Anything that
stops the critical section from finishing also stops the heartbeat from firing.
It shortens the *typical* lease, which is genuinely useful for failover speed
(see the leader election), and does nothing for the case that actually hurts.

**Fencing token.** `acquire` returns a strictly increasing integer. Every write
the holder makes carries it, and the resource being written to rejects any
token lower than the highest it has already accepted.

## Decision

Fencing token, with `extend` available alongside it.

The reasoning that settles it: the overlap is not preventable. Two processes
*will* sometimes both believe they hold the lock, because that belief lives in
process memory and Redis cannot reach it. What is preventable is the stale
process's *effect* on the world. So stop trying to make the lock stronger and
make the resource able to refuse.

This also relocates the guarantee to somewhere it can actually be enforced. The
lock is advisory - nothing forces a process to hold it. The resource is not
advisory; every write goes through it by definition.

## Consequences

- `acquire` returns a `Lease`, not a boolean, and `withLock` passes the lease
  into the callback rather than hiding it. A `withLock` that hides the token
  hides the only defence the caller has.
- The fencing counter key has no TTL and must never be evicted. If it is lost,
  `INCR` restarts at 1 and tokens are reused; every claim here collapses.
  `docker-compose.yml` sets `maxmemory-policy noeviction`. Asserted in
  `tests/fencing.test.ts` ("loses every guarantee if the fencing counter is
  lost") so the failure mode is visible rather than folkloric.
- **The token is useless unless the downstream resource checks it.** Handing
  out tokens and not checking them is strictly worse than not having them,
  because it looks like protection.

## Limits of what this buys

Fencing orders writes; it does not detect staleness on its own. If the expired
holder writes *before* the new holder has written anything, the resource has
never seen a higher token and has no basis to reject - the stale write is
accepted. Asserted in `tests/fencing.test.ts` ("does NOT reject a stale writer
that gets there first"). What fencing guarantees is that once a newer holder's
write lands, no older holder can ever overwrite it.

Resources that cannot express a compare-on-token check cannot be fenced at all:
a plain S3 PUT, an append-only log, an outbound webhook to a third party. For
those, a lock is best-effort no matter how the lock is implemented, and the
right move is usually to make the operation idempotent instead.
