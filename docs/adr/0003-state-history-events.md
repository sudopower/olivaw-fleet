# ADR 0003: Current state, history and events are different data

Status: accepted

## Context

At one report per second, every robot produces 86,400 messages a day.
Storing each one as it arrives is the obvious design and the wrong one: it
spends almost all of the system's write capacity on data nobody reads. The
fleet manager's question is "what is the fleet doing right now", and the
answer to that is small.

Looking at what is actually consumed, telemetry splits into three kinds of
data with different volumes, consumers and tolerance for loss:

| | Current state | History | Events |
|---|---|---|---|
| What | latest sample per robot | samples over time | alert raised or cleared, connectivity change |
| Read by | fleet overview, robot detail | battery chart | alert list, later ticketing |
| Volume | one row per robot | decimated | rare |
| Under load | coalesce: keep the latest | decimate | never drop |
| On write failure | retry with the latest | drop | keep all, retry |

## Decision

The pipeline treats each kind according to its column:

- **Current state is coalesced.** Robots that changed are marked dirty, and
  each flush (1 s) writes one row per dirty robot. Ten reports between
  flushes cost one row update.
- **History is decimated at ingest.** A sample is kept if the robot's last
  kept sample is `HistoryInterval` old (5 s), or if it changed mode or caused
  an event. Charts keep their shape and their interesting points at a fifth
  of the rows.
- **Events are never dropped.** The domain derives them from the full
  sample stream. They are written in the same transaction as the state that
  caused them, and kept in memory and retried if the write fails. Their
  writes are idempotent, so a retry after an ambiguous commit cannot create
  duplicates.

Samples carry full state rather than deltas. That is what makes coalescing
and dropping safe: the next sample restores everything a dropped one held.
When the inbox is full, new samples are dropped and counted rather than
blocking the MQTT client.

## Consequences

- Write volume scales with the number of robots, not with message rate.
- The fleet view lags reality by up to one flush interval. Fine for a human
  watching a console.
- A crash loses at most one flush interval of state and history. Events
  from that interval are re-derived when robots next report, because the
  restored state still holds the alerts that were active.
- Pending events are held in memory while the database is down. A long
  outage during a mass event (a whole site losing power) could hold a lot of
  them. In production they would spill to disk or a durable queue.
- Decimation means history cannot answer "what exactly happened at
  12:00:03". If someone needs that, raw samples belong in cheap object
  storage, not in the database serving the console.
