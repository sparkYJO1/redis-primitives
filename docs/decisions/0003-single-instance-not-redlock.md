# 3. Single-instance Redis, and explicitly not Redlock

Status: accepted
Date: 2026-09-06

## Context

The obvious objection to a Redis lock is that one Redis instance is a single
point of failure. Redlock is the standard answer: run five independent Redis
masters, acquire on a majority, and treat the lock as held if you got 3 of 5
within a bounded time.

Adopting Redlock would let this repo claim more. It would be the wrong claim.

## Decision

One Redis instance. No Redlock. No consensus claim anywhere in the README, and
the guarantees stated in terms of what actually holds.

## Reasoning

**Redlock does not remove the need for fencing, so it does not remove the
problem this repo is about.** Redlock's own documentation acknowledges that a
client can be paused past its lease expiry. Whether the lease came from one
node or from a majority of five, the holder can still be inside the critical
section after it expires, and the resource still has to reject the stale
writer. Since fencing is required either way, Redlock's five nodes buy
availability, not safety - and availability was not the thing that was broken.

**The extra safety it appears to buy depends on assumptions that are not
testable here.** Redlock's argument rests on bounded clock drift across the
five nodes and bounded message delay. A single NTP step, a VM restored from a
snapshot, or a leap-second smear on one node can expire that node's key early;
enough of those at once and two clients hold a majority simultaneously. This is
the substance of Kleppmann's critique and Antirez's reply, and the honest
summary is that the two disagree about which failure model is realistic, not
about the mechanism. A repo that cannot test its own timing assumptions should
not be asserting the stronger claim.

**Redlock is five times the operational surface for a weaker guarantee than
people believe it gives.** Five independent masters - not a cluster, not
replicas - to run, monitor and patch, plus clock-drift monitoring across all
five, plus a majority-acquire path with its own retry and clock-validity
arithmetic. That is a lot of machinery to protect a claim that still has to be
qualified in the README.

**If the requirement really is consensus, Redis is the wrong tool.** etcd,
ZooKeeper and Consul implement Raft or ZAB, elect leaders with epochs, and have
been attacked by people whose job is attacking them. "Redis with a TTL" and
"a consensus system" are different categories, and a lease is not a quorum.

## What this repo therefore claims

Mutual exclusion holds while the Redis instance is up and its clock advances
monotonically. If Redis fails over to a replica, replication is asynchronous,
so a lock acknowledged by the old master may not exist on the new one and two
holders can appear. If Redis is unreachable, `acquire` throws and no lock is
held. Nothing here survives a partition, and nothing here is consensus.

The fencing token is what keeps the failure containable rather than silent:
under a replica failover the token counter can go backwards too - the same
failure as losing the counter key - so a failover is not a scenario this design
survives, it is a scenario it is honest about.

## Consequences

- Suitable for: preventing duplicate cron runs, serialising work per tenant,
  keeping one worker on a queue - where a rare duplicate is a cost, not a
  catastrophe, and where the resource can be fenced or the work made
  idempotent.
- Not suitable for: anything where a duplicate is unacceptable and cannot be
  detected downstream. Use a database transaction if the work is in one
  database, or a real consensus system if it is not.
- Redis should be configured with `maxmemory-policy noeviction` and, if the
  fencing counter must survive restarts, with persistence enabled.
