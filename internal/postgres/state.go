package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/sudopower/olivaw-fleet/internal/fleet"
	"github.com/sudopower/olivaw-fleet/internal/ingest"
)

// StateStore implements ingest.Store.
type StateStore struct{ db *DB }

func (db *DB) State() *StateStore { return &StateStore{db: db} }

// WriteBatch writes one flush in a single transaction, so readers never see
// an alert for a robot whose state has not caught up, or the reverse.
func (s *StateStore) WriteBatch(ctx context.Context, b ingest.Batch) error {
	return pgx.BeginFunc(ctx, s.db.pool, func(tx pgx.Tx) error {
		if err := upsertState(ctx, tx, b.Snapshots); err != nil {
			return fmt.Errorf("upsert state: %w", err)
		}
		if err := copyHistory(ctx, tx, b.History); err != nil {
			return fmt.Errorf("copy history: %w", err)
		}
		if err := applyEvents(ctx, tx, b.Events); err != nil {
			return fmt.Errorf("apply events: %w", err)
		}
		return nil
	})
}

// upsertState writes many rows in one statement by passing each column as an
// array and unnesting it server-side: one round trip, no temp table.
//
// The WHERE guard keeps an older snapshot from overwriting a newer one. With
// one writer that cannot happen today; it matters once robots are spread
// across instances (ADR 0005).
func upsertState(ctx context.Context, tx pgx.Tx, snaps []fleet.Snapshot) error {
	n := len(snaps)
	var (
		ids, sites, conns, modes, maps, faults = make([]string, 0, n), make([]string, 0, n), make([]string, 0, n), make([]string, 0, n), make([]string, 0, n), make([]string, 0, n)
		disc, charging                         = make([]bool, 0, n), make([]bool, 0, n)
		lastSeen, deviceTime, received         = make([]time.Time, 0, n), make([]time.Time, 0, n), make([]time.Time, 0, n)
		battery                                = make([]float32, 0, n)
		xs, ys, headings                       = make([]float64, 0, n), make([]float64, 0, n), make([]float64, 0, n)
	)
	for _, s := range snaps {
		if !s.HasSample {
			continue
		}
		f, err := json.Marshal(faultsJSON(s.Last.Faults))
		if err != nil {
			return err
		}
		ids = append(ids, string(s.RobotID))
		sites = append(sites, string(s.SiteID))
		conns = append(conns, string(s.Connectivity))
		disc = append(disc, s.Disconnected)
		lastSeen = append(lastSeen, s.LastSeen)
		deviceTime = append(deviceTime, s.Last.DeviceTime)
		received = append(received, s.Last.ReceivedAt)
		modes = append(modes, string(s.Last.Mode))
		battery = append(battery, float32(s.Last.Battery.Percent))
		charging = append(charging, s.Last.Battery.Charging)
		maps = append(maps, s.Last.Pose.MapID)
		xs = append(xs, s.Last.Pose.X)
		ys = append(ys, s.Last.Pose.Y)
		headings = append(headings, s.Last.Pose.Heading)
		faults = append(faults, string(f))
	}
	if len(ids) == 0 {
		return nil
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO robot_state AS r (robot_id, site_id, connectivity, disconnected, last_seen_at, device_time,
			received_at, mode, battery_pct, charging, map_id, x, y, heading, faults, updated_at)
		SELECT u.id, u.site, u.conn, u.disc, u.seen, u.dev, u.recv, u.mode, u.bat, u.chg, u.map, u.x, u.y, u.h,
			u.faults::jsonb, now()
		FROM unnest($1::text[], $2::text[], $3::text[], $4::bool[], $5::timestamptz[], $6::timestamptz[],
			$7::timestamptz[], $8::text[], $9::real[], $10::bool[], $11::text[], $12::float8[], $13::float8[],
			$14::float8[], $15::text[])
			AS u(id, site, conn, disc, seen, dev, recv, mode, bat, chg, map, x, y, h, faults)
		ON CONFLICT (robot_id) DO UPDATE SET
			site_id = EXCLUDED.site_id, connectivity = EXCLUDED.connectivity,
			disconnected = EXCLUDED.disconnected, last_seen_at = EXCLUDED.last_seen_at,
			device_time = EXCLUDED.device_time, received_at = EXCLUDED.received_at, mode = EXCLUDED.mode,
			battery_pct = EXCLUDED.battery_pct, charging = EXCLUDED.charging, map_id = EXCLUDED.map_id,
			x = EXCLUDED.x, y = EXCLUDED.y, heading = EXCLUDED.heading, faults = EXCLUDED.faults,
			updated_at = EXCLUDED.updated_at
		WHERE r.last_seen_at <= EXCLUDED.last_seen_at`,
		ids, sites, conns, disc, lastSeen, deviceTime, received, modes, battery, charging, maps, xs, ys, headings, faults)
	return err
}

// copyHistory streams history rows with COPY, the fastest way into Postgres.
func copyHistory(ctx context.Context, tx pgx.Tx, samples []fleet.Sample) error {
	if len(samples) == 0 {
		return nil
	}
	_, err := tx.CopyFrom(ctx, pgx.Identifier{"telemetry"},
		[]string{"robot_id", "received_at", "device_time", "mode", "battery_pct", "charging", "map_id", "x", "y", "heading"},
		pgx.CopyFromSlice(len(samples), func(i int) ([]any, error) {
			s := samples[i]
			return []any{string(s.RobotID), s.ReceivedAt, s.DeviceTime, string(s.Mode), float32(s.Battery.Percent),
				s.Battery.Charging, s.Pose.MapID, s.Pose.X, s.Pose.Y, s.Pose.Heading}, nil
		}))
	return err
}

// applyEvents persists alert transitions in order. Connectivity changes are
// not stored as rows: the offline alert already records every outage that
// matters, and the current connectivity lives in robot_state.
//
// Both statements are idempotent, so replaying a batch after a failed commit
// is safe: raising hits the partial unique index and does nothing; clearing
// only touches an alert that is still active.
func applyEvents(ctx context.Context, tx pgx.Tx, events []fleet.Event) error {
	var batch pgx.Batch
	for _, e := range events {
		a := e.Alert
		switch e.Type {
		case fleet.EventAlertRaised:
			batch.Queue(`
				INSERT INTO alerts (robot_id, site_id, kind, code, severity, message, raised_at)
				VALUES ($1, $2, $3, $4, $5, $6, $7)
				ON CONFLICT (robot_id, kind, code) WHERE cleared_at IS NULL DO NOTHING`,
				e.RobotID, e.SiteID, a.Key.Kind, a.Key.Code, a.Severity, a.Message, a.RaisedAt)
		case fleet.EventAlertCleared:
			batch.Queue(`
				UPDATE alerts SET cleared_at = $4
				WHERE robot_id = $1 AND kind = $2 AND code = $3 AND cleared_at IS NULL`,
				e.RobotID, a.Key.Kind, a.Key.Code, e.At)
		}
	}
	if batch.Len() == 0 {
		return nil
	}
	return tx.SendBatch(ctx, &batch).Close()
}

// LoadSnapshots restores every robot's last persisted state and active
// alerts, for the pipeline to resume from after a restart.
func (s *StateStore) LoadSnapshots(ctx context.Context) ([]fleet.Snapshot, error) {
	rows, err := s.db.pool.Query(ctx, `
		SELECT robot_id, site_id, connectivity, disconnected, last_seen_at, device_time, received_at,
			mode, battery_pct, charging, map_id, x, y, heading, faults
		FROM robot_state`)
	if err != nil {
		return nil, err
	}
	snaps, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (fleet.Snapshot, error) {
		var (
			s      fleet.Snapshot
			bat    float32
			faults []byte
		)
		err := row.Scan(&s.RobotID, &s.SiteID, &s.Connectivity, &s.Disconnected, &s.LastSeen,
			&s.Last.DeviceTime, &s.Last.ReceivedAt, &s.Last.Mode, &bat, &s.Last.Battery.Charging,
			&s.Last.Pose.MapID, &s.Last.Pose.X, &s.Last.Pose.Y, &s.Last.Pose.Heading, &faults)
		if err != nil {
			return s, err
		}
		s.Last.RobotID = s.RobotID
		s.Last.Battery.Percent = float64(bat)
		s.HasSample = true
		var fj []faultJSON
		if err := json.Unmarshal(faults, &fj); err != nil {
			return s, err
		}
		s.Last.Faults = fromFaultsJSON(fj)
		return s, nil
	})
	if err != nil {
		return nil, fmt.Errorf("load state: %w", err)
	}

	idx := make(map[fleet.RobotID]int, len(snaps))
	for i, s := range snaps {
		idx[s.RobotID] = i
	}
	rows, err = s.db.pool.Query(ctx, `
		SELECT robot_id, kind, code, severity, message, raised_at FROM alerts WHERE cleared_at IS NULL`)
	if err != nil {
		return nil, err
	}
	var (
		id fleet.RobotID
		a  fleet.Alert
	)
	_, err = pgx.ForEachRow(rows, []any{&id, &a.Key.Kind, &a.Key.Code, &a.Severity, &a.Message, &a.RaisedAt}, func() error {
		if i, ok := idx[id]; ok {
			snaps[i].Alerts = append(snaps[i].Alerts, a)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("load alerts: %w", err)
	}
	return snaps, nil
}

// faultJSON is the stored shape of a fault, decoupled from the domain type
// so renaming a Go field cannot silently change stored data.
type faultJSON struct {
	Code     string `json:"code"`
	Severity string `json:"severity"`
	Message  string `json:"message,omitempty"`
}

func faultsJSON(fs []fleet.Fault) []faultJSON {
	out := make([]faultJSON, len(fs))
	for i, f := range fs {
		out[i] = faultJSON{Code: f.Code, Severity: string(f.Severity), Message: f.Message}
	}
	return out
}

func fromFaultsJSON(fs []faultJSON) []fleet.Fault {
	if len(fs) == 0 {
		return nil
	}
	out := make([]fleet.Fault, len(fs))
	for i, f := range fs {
		out[i] = fleet.Fault{Code: f.Code, Severity: fleet.Severity(f.Severity), Message: f.Message}
	}
	return out
}
