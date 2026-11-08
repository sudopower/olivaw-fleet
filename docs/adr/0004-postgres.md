# ADR 0004: PostgreSQL for everything, shaped per kind of data

Status: accepted

## Context

The stack offers MongoDB and PostgreSQL. The data here is:

- a registry (sites, robots) that is relational and needs uniqueness and
  foreign keys,
- one hot row per robot, rewritten every second,
- append-only, time-ordered history,
- alerts with a "one active per robot and kind" invariant, read by site.

On-prem customers have to run whatever we pick, so one database beats two.

## Decision

Use PostgreSQL for all of it, and shape each table to its access pattern:

- **`robot_state`**: one row per robot, upserted in bulk with
  `INSERT ... SELECT FROM unnest(arrays) ON CONFLICT DO UPDATE`: one
  statement per flush however many robots changed. `fillfactor = 70` and
  indexes only on columns that rarely change keep updates HOT (no index
  writes).
- **`telemetry`**: a plain table indexed on `(robot_id, received_at)`,
  written with `COPY`.
- **`alerts`**: a partial unique index on `(robot_id, kind, code) WHERE
  cleared_at IS NULL` enforces the domain invariant in the database and
  makes alert writes idempotent on retry.
- One transaction per flush, so state and alerts never disagree.

## Why not MongoDB

Mongo would handle the hot state collection fine, and its time-series
collections would handle history. But the registry and alerts want
relational constraints, and I would rather enforce "one active alert" in an
index than in application code. Running one database on-prem also halves
the operational surface. Mongo stays a good fit for genuinely
document-shaped data, such as vendor-specific robot configuration.

## Consequences

- History has no retention yet. The next step is daily partitions dropped
  whole, or TimescaleDB (same SQL, compressed chunks, continuous aggregates
  for the charts) rather than a different database.
- The per-second upsert is the most expensive thing the system does, and
  the first thing to break with scale (ADR 0005).
- Migrations are a tiny forward-only runner with an advisory lock, not a
  library. Enough for one service.
