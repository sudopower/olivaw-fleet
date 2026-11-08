-- Registry: low volume, relational, strongly consistent.

CREATE TABLE sites (
    id         text PRIMARY KEY,
    name       text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE robots (
    id            text PRIMARY KEY,
    site_id       text NOT NULL REFERENCES sites (id),
    manufacturer  text NOT NULL,
    serial_number text NOT NULL,
    name          text NOT NULL,
    model         text NOT NULL DEFAULT '',
    created_at    timestamptz NOT NULL DEFAULT now(),
    UNIQUE (manufacturer, serial_number)
);

CREATE INDEX robots_site_id ON robots (site_id, id);

-- Current state: one row per robot, overwritten about once per second.
--
-- This is the hottest table in the system. Two choices keep updates cheap:
--   * fillfactor leaves room on each page so an update can be written next to
--     the old row version (a HOT update) instead of touching every index.
--   * only columns that rarely change are indexed. Indexing battery_pct would
--     make every update non-HOT. connectivity changes rarely enough to index.
CREATE TABLE robot_state (
    robot_id      text PRIMARY KEY REFERENCES robots (id),
    site_id       text NOT NULL,          -- denormalized: every read is per site
    connectivity  text NOT NULL,          -- as judged by the ingest pipeline
    disconnected  boolean NOT NULL,       -- broker reported the connection lost
    last_seen_at  timestamptz NOT NULL,   -- server clock
    device_time   timestamptz NOT NULL,   -- robot clock, last accepted sample
    received_at   timestamptz NOT NULL,   -- server clock, last accepted sample
    mode          text NOT NULL,
    battery_pct   real NOT NULL,
    charging      boolean NOT NULL,
    map_id        text NOT NULL,
    x             double precision NOT NULL,
    y             double precision NOT NULL,
    heading       double precision NOT NULL,
    faults        jsonb NOT NULL,
    updated_at    timestamptz NOT NULL DEFAULT now()
) WITH (fillfactor = 70);

CREATE INDEX robot_state_site_connectivity ON robot_state (site_id, connectivity, robot_id);

-- History: decimated samples for trend charts. Indexed on the server clock,
-- not the robot clock, so a robot with a wrong clock cannot hide its history.
-- At scale this table is partitioned by day so retention is a cheap DROP
-- instead of a DELETE (ADR 0005); at this size a plain table is enough.
CREATE TABLE telemetry (
    robot_id     text NOT NULL,
    received_at  timestamptz NOT NULL,
    device_time  timestamptz NOT NULL,
    mode         text NOT NULL,
    battery_pct  real NOT NULL,
    charging     boolean NOT NULL,
    map_id       text NOT NULL,
    x            double precision NOT NULL,
    y            double precision NOT NULL,
    heading      double precision NOT NULL
);

CREATE INDEX telemetry_robot_time ON telemetry (robot_id, received_at);

-- Alerts: rare, must not be lost, and what a fleet manager acts on.
CREATE TABLE alerts (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    robot_id    text NOT NULL REFERENCES robots (id),
    site_id     text NOT NULL,
    kind        text NOT NULL,
    code        text NOT NULL DEFAULT '',
    severity    text NOT NULL,
    message     text NOT NULL,
    raised_at   timestamptz NOT NULL,
    cleared_at  timestamptz
);

-- The domain invariant "at most one active alert per robot and key", enforced
-- by the database too. It also makes raising an alert idempotent: replaying a
-- flush after a crash cannot create a duplicate.
CREATE UNIQUE INDEX alerts_one_active ON alerts (robot_id, kind, code) WHERE cleared_at IS NULL;
CREATE INDEX alerts_active_by_site ON alerts (site_id, raised_at DESC, id DESC) WHERE cleared_at IS NULL;
CREATE INDEX alerts_by_robot ON alerts (robot_id, raised_at DESC, id DESC);
