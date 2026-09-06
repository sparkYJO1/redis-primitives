# 2. Lua scripts, not WATCH/MULTI

Status: accepted
Date: 2026-09-06

Records an approach that was tried and abandoned, with the measurements.

## Context

Every primitive here needs read-then-decide-then-write to happen as one
indivisible step:

- lock acquire: is it free? then take it and stamp a token on it
- rate limiter: how many are in the window? then admit and record
- fenced write: what is the highest token seen? then accept and store

Redis has a built-in answer for this - `WATCH` for optimistic concurrency,
`MULTI`/`EXEC` for atomic execution - and it needs no scripting. That was the
first implementation.

## What was tried

The sliding-window limiter, written with WATCH/MULTI:

```ts
await client.watch(key);
const used = await client.zcount(key, `(${now - windowMs}`, "+inf");
if (used >= limit) { await client.unwatch(); return false; }
const result = await client.multi()
  .zremrangebyscore(key, "-inf", now - windowMs)
  .zadd(key, now, randomUUID())
  .pexpire(key, windowMs)
  .exec();
return result !== null;   // null means WATCH aborted the transaction
```

`MULTI` on its own is not enough and never was: it is atomic *execution*, not
read-then-decide. The count has to come back to the client before the client
can decide, and `MULTI` queues commands rather than returning intermediate
results. So the decision genuinely depends on `WATCH`.

## What happened

`experiments/watch-multi-race.ts`, 100 concurrent callers, limit 5, run on
Redis 7.4. Three variants, reproducible with `npm run experiment:watch-multi`:

```
A  shared connection      admitted 100/100  (limit 5)
B  connection per caller  admitted 5/100    (limit 5)  259 retries, 100 connections, 454ms
C  Lua, shared connection admitted 5/100    (limit 5)  0 retries, 1 connection, 23ms
```

Variant A is the failure, and it is worse than a performance problem: **the
limiter did not limit anything.** Every one of 100 callers was admitted against
a limit of 5. Across repeated runs A admits 100/100 every time.

The cause is that `WATCH` state belongs to the *connection*, not to the logical
caller, and `EXEC` clears every watch on that connection. ioredis multiplexes -
one client, many concurrent commands - which is how essentially every Node
service is wired. So the interleaving is:

```
caller 1: WATCH key
caller 2: WATCH key
caller 1: EXEC        -> succeeds, and clears the connection's watch list
caller 2: EXEC        -> its WATCH is already gone, so nothing aborts it
```

Caller 2's transaction cannot abort, because from the connection's point of
view there is nothing being watched any more. The guard silently evaporates.
Nothing throws. The code looks correct in review, passes a single-threaded
test, and fails only under concurrency - which is the only condition it exists
for.

Variant B is the fix that keeps WATCH: give every in-flight operation its own
connection. It is correct. It also costs one Redis connection per concurrent
request and 259 retries for 100 admissions, because under contention nearly
every attempt is invalidated by a competing write. For a rate limiter, which
runs on the hot path of every request, a connection per request is not a
trade-off, it is a non-starter.

## Decision

All read-then-decide logic goes in Lua, loaded with `defineCommand` (EVALSHA
with an EVAL fallback, so the body crosses the wire once per server rather than
once per call).

Redis runs a script to completion with nothing interleaved. That is the same
guarantee `WATCH` was being used to approximate, obtained by construction
instead of by retry, and it is connection-agnostic - one shared client is fine,
which is the deployment everyone actually has.

## Consequences

- The interesting logic is in Lua string literals, so TypeScript does not check
  it and the editor does not highlight it. Mitigated by keeping every script
  short enough to read in one screen and testing all of them against real
  Redis. There is no unit-test substitute; a mock cannot tell you whether a
  script is atomic.
- A long script blocks the whole server, since Redis is single-threaded. All
  scripts here are O(log N) plus a bounded trim.
- `TIME` inside a script makes it non-deterministic. Safe on Redis 5+, which
  replicates effects rather than the script body. On Redis 4 or earlier it
  would need `redis.replicate_commands()` first.
- Ported to a language whose Redis client does not multiplex - a Go service
  with a real connection pool - variant A's failure would not reproduce, and
  the WATCH version would look fine. The bug is in the interaction between
  WATCH's connection scope and the client's concurrency model, not in Redis.
  Worth knowing before assuming this ADR generalises.
