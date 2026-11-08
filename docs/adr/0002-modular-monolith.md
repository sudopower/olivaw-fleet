# ADR 0002: One service with strict module boundaries

Status: accepted

## Context

The system has three jobs: take in telemetry, derive the state of the fleet
from it, and serve that state to a console. These could be three services
(ingest, state, API) connected by a queue, or one process.

The platform must also run on-prem, on a customer's own servers, where every
extra moving part is something the customer has to operate.

## Decision

Build one Go binary, `fleetd`, with the jobs split into packages that only
talk through narrow interfaces:

| Package              | Owns                                                        |
|----------------------|-------------------------------------------------------------|
| `internal/fleet`     | The domain: robot state, liveness, alert rules. Pure, no IO. |
| `internal/telemetry` | Vendor adapters that turn wire formats into `fleet.Sample`.  |
| `internal/ingest`    | The pipeline: routing, sharding, batching, backpressure.     |
| `internal/registry`  | Which robots exist and which site they belong to.            |
| `internal/postgres`  | Storage, behind interfaces owned by the packages that use it. |
| `internal/api`       | The HTTP and live-stream interface for the presentation layer. |

The domain package imports nothing from the others. Storage interfaces are
declared by their consumers, not by `postgres`.

## Consequences

- Deployable as one binary plus Postgres and an MQTT broker. Same artifact in
  the cloud and on-prem.
- No network hop or serialization between ingest and state, which matters at
  100k messages per second.
- The split point is already visible: ingest is sharded by robot ID, so at
  larger scale each `fleetd` instance owns a range of robots (see ADR 0005).
  The API can be split out because it only reads Postgres and a live feed.
- Cost: one process means one failure domain. A crash stops both ingest and
  reads until restart. Acceptable at this stage, and the restart path is
  designed to recover state from Postgres.
