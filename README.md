# olivaw-fleet

Backend for a robot fleet console. Robots report their state about once per
second over MQTT. This service works out what the fleet is doing right now,
raises the things a fleet manager has to act on, and serves both over a REST
API for a presentation layer.

```
make up                                   # Postgres + Mosquitto in Docker
go run ./cmd/fleetd                       # the service, on :8080
go run ./cmd/simulator -robots 300        # a misbehaving fleet, in another terminal
curl -s localhost:8080/v1/summary | jq
```

## What I chose to build, and why

The server side. The robots are faked by a small simulator, as the brief
allows, and there is no UI. "What is the fleet doing right now" sounds like
a read query, but most of the hard parts are in deciding what is true:

1. **Silence is the most important signal.** A robot that dies cannot report
   that it died. The service judges connectivity itself from the absence of
   data (stale after 5 s, offline after 30 s), uses the MQTT last-will for
   faster detection, and stays correct across its own restarts.
2. **Most telemetry is not worth storing.** The answer to "what is happening
   now" is one row per robot, and what people act on is a handful of
   events. Treating current state, history and events as three different
   kinds of data ([ADR 0003](docs/adr/0003-state-history-events.md)) is the
   core of the design.
3. **Robot data is messy.** Messages arrive twice, out of order, with
   drifting clocks. The domain handles that in plain, tested code.

## How it fits together

```
robots ──MQTT──► Mosquitto ──► fleetd
                                 │ route topic, resolve robot (registry)
                                 ▼
                     pipeline goroutine: decode, validate,
                     fleet.Robot.Observe / Tick ──► events
                                 │ every 1 s, one transaction
                                 ▼
                             Postgres ◄── query model ◄── REST API
```

| Package | What it owns |
|---|---|
| `internal/fleet` | The domain: ordering, liveness, battery and fault alerts, restart recovery. No IO, imports nothing else in the module. |
| `internal/telemetry` | Vendor adapters that turn wire formats into a `fleet.Sample`. VDA 5050 is implemented. |
| `internal/ingest` | The pipeline: coalesce state, decimate history, never drop events, retry. |
| `internal/registry` | Which robots exist and which site they belong to. |
| `internal/postgres` | Storage. Interfaces are declared by the packages that use them. |
| `internal/query`, `internal/api` | The read model and the HTTP interface. |
| `cmd/simulator` | The fake fleet: one file that publishes VDA 5050 state and misbehaves on the wire. |

One binary, `fleetd`, with strict package boundaries
([ADR 0002](docs/adr/0002-modular-monolith.md)). The platform also runs
on-prem, where every extra service is something a customer has to operate,
so the split into services waits until scale demands it
([ADR 0005](docs/adr/0005-scaling-path.md)).

MQTT because it is what VDA 5050 specifies and what robot vendors already
ship, and because the broker's last-will tells us when a connection dies.

Postgres for everything, each table shaped for its access pattern, and why
not Mongo: [ADR 0004](docs/adr/0004-postgres.md).

## What goes wrong, and what handles it

The simulator injects the first nine; the last two you cause by stopping a
process.

| Case | Handled by | Observable as |
|---|---|---|
| Duplicate delivery (1%) | `Robot.Observe`: same device time | `result="duplicate"` metric |
| Reordering (0.5%) | `Robot.Observe`: older device time | `result="out_of_order"` |
| Robot clock off by up to ±2 min | Ordering uses the robot's clock, liveness uses the server's | `clock_skew_ms` on the robot |
| Robot clock jumps back a minute | Resync after 10 s of server time | robot does not get stuck |
| Robot goes silent | Stale at 5 s, offline at 30 s | `offline` alert |
| Connection breaks (last-will) | Offline immediately | `offline` alert |
| Battery cut too fine | Thresholds 20 % and 10 % with hysteresis, only when not charging | `battery_low`, `battery_critical` |
| Faults raised and cleared | Diff of the reported fault set | `fault` alerts with vendor code |
| Unprovisioned robot | Registry lookup | `result="unknown_robot"`, dropped |
| fleetd restart | State and active alerts restored; silence timed from restart | no alert storm, no false offline |
| Postgres down | State and events retried; history dropped | `fleet_ingest_flush_errors_total` |

Running it found two bugs in the domain that unit tests had not: after a
restart, "last seen" showed the restart time instead of the robot's last
report, and robots already charging raised battery alerts. Both fixes are
separate commits with tests.

On a laptop (M2 Pro, everything local), 20,000 simulated robots at 1 Hz
cost fleetd about 0.6 cores with nothing dropped; the 1 s flush takes about
200 ms, nearly all of it the Postgres upsert. That is where scaling starts
(ADR 0005).

## API

| | |
|---|---|
| `GET /v1/summary?site_id=` | Per site and in total: robots by connectivity and mode, low batteries, active and critical alerts |
| `GET /v1/robots?site_id=&connectivity=&limit=` | Robots with current state, those with active alerts first |
| `GET /v1/robots/{id}` | One robot, its state and recent alerts |
| `GET /v1/robots/{id}/telemetry?window=1h` | Battery and mode history in about 120 buckets |
| `GET /v1/alerts?site_id=&robot_id=&state=active\|all&limit=` | Alerts, newest first |
| `POST /v1/sites`, `POST /v1/robots` | Provisioning |
| `GET /healthz`, `/readyz`, `/metrics` | Operations |

Connectivity is judged again at read time: if the pipeline itself stops, a
silent robot reads as offline even though its stored row still says online.

## What I left out, and why

- **No UI.** The API is shaped for one (an overview from the summary, a list
  with problems first, a detail view with a battery chart), but the backend
  is where I wanted to spend the time.
- **No live stream.** A console polling the summary every few seconds is
  enough to start. A stream needs per-client subscriptions and a policy for
  slow clients; I would build it together with the UI that uses it.
- **No auth or tenancy.** Every caller sees every site. In production:
  per-device MQTT credentials with topic ACLs, and tenant-scoped API tokens.
- **No commands to robots.** Monitoring only. Sending orders is a different
  consistency problem (acknowledgements, timeouts, idempotency).
- **One vendor protocol.** The adapter interface is there; a second adapter
  is one file, and should be written against real captured payloads.
- **No pagination.** A site has hundreds of robots, and the list is filtered
  by site and sorted by what needs attention.
- **No history retention.** One plain table. Daily partitions dropped whole
  are the next step.
- **Fake robots.** They drain, charge and fault, but do not drive routes or
  travel to a dock. Enough to exercise every rule in fleetd, and small
  enough to read in a few minutes.
- **Simulated last-will.** The simulator shares one MQTT connection
  between all its robots, so it publishes the `CONNECTIONBROKEN` message
  itself instead of the broker doing it. fleetd cannot tell the difference.

## With more time, or in production

- Write state only when it changed materially: the cheapest win on the
  current bottleneck.
- Spill pending events to disk while Postgres is down, instead of holding
  them in memory.
- Alert escalation (offline for 5 minutes pages someone) and per-site
  thresholds: a hospital wants different ones from a parking garage.
- Contract tests per adapter against captured payloads from real robots.
- Tracing across the flush path and a dashboard over the existing metrics.

## Development

```
make up                 # dependencies
make test               # unit tests
make test-integration   # plus Postgres integration tests (throwaway DB per test)
make lint
```
