# ADR 0005: What is deliberately not built, and the path to 100x

Status: accepted (as a plan)

## Context

Fleets today are in the thousands of robots; the target is hundreds of
thousands. At 1 Hz that is 100k+ messages per second. This version is one
process with one pipeline goroutine and one Postgres. This ADR records where
that breaks and what changes, in order, so the simple version is a
deliberate first step and not a dead end.

Measured on an M2 Pro laptop with everything local: 20,000 robots at 1 Hz
cost fleetd about 0.6 cores, and the 1 s flush takes about 200 ms, nearly
all of it the Postgres upsert. Nothing was dropped.

## What breaks first, and the fix for each

1. **The state upsert.** Rewriting one row per robot per second is the
   most expensive thing the system does. Fixes, cheapest first:
   - Skip robots whose state did not change materially (same mode, battery
     within 1 %, moved less than 0.5 m). Most robots most of the time are
     doing what they did a second ago.
   - Stop treating Postgres as the source of current state: serve reads from
     the pipeline's memory, which already holds it, and checkpoint to
     Postgres every 10 to 30 s for recovery.
2. **One goroutine.** The pipeline handles messages and flushes on the same
   goroutine, so a slow flush backs up the inbox. Next step: partition
   robots by `hash(robot_id)` over N goroutines, each owning its robots, with
   a separate flusher. Every robot still has exactly one owner, so the
   domain code does not change.
3. **One process.** The same partitioning, made external: each instance owns
   a range of partitions, and a transport with keyed partitions (Kafka, or
   NATS JetStream) routes each robot to its owner. MQTT shared subscriptions
   do not keep a robot on one consumer. The `WHERE last_seen_at <=
   EXCLUDED.last_seen_at` guard on the upsert already makes handover between
   owners safe.
4. **The MQTT broker.** One Mosquitto handles tens of thousands of
   connections. Past that: a clustered broker (EMQX, HiveMQ).
5. **Reads.** Site summaries scan a site's robots. Fine up to a few thousand
   per site; past that, the pipeline maintains the counters (it sees every
   transition) and serves them from memory.
6. **History.** Daily partitions so retention is a `DROP`, then TimescaleDB
   or a columnar store.

## Also left out on purpose

- **A live stream (SSE or WebSocket).** A console polling the summary every
  few seconds is enough to start. A stream needs per-client subscriptions
  and a policy for slow clients, and is worth building with the UI that
  consumes it.
- **A second vendor protocol.** The adapter interface is there and VDA 5050
  is implemented. A second adapter is one file; I would rather write it
  against captured payloads from a real robot than an invented protocol.

## On-prem

The same binary runs on-prem with Postgres and Mosquitto, which is why
none of the above is built yet: every step adds infrastructure a customer
must run. The intended split is a small profile (this design, up to tens of
thousands of robots) and a large profile (partitioned transport, many
instances) for the cloud, sharing the domain and adapters unchanged.
