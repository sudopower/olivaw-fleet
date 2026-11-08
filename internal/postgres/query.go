package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/sudopower/olivaw-fleet/internal/fleet"
	"github.com/sudopower/olivaw-fleet/internal/query"
)

// QueryStore implements query.Store.
type QueryStore struct {
	db     *DB
	policy fleet.Policy
}

// Query returns the read model. The policy must match the pipeline's: it is
// used to judge connectivity at read time and to count low batteries.
func (db *DB) Query(p fleet.Policy) *QueryStore { return &QueryStore{db: db, policy: p} }

// effectiveConnectivity is the stored connectivity, overridden when the robot
// has been silent past the offline threshold. The stored value is written by
// the pipeline; if the pipeline is down, nothing updates it, and without
// this override a dead fleet would keep reading as online.
const effectiveConnectivity = `CASE
	WHEN s.robot_id IS NULL THEN 'never_seen'
	WHEN s.last_seen_at < now() - @offline_after::interval THEN 'offline'
	ELSE s.connectivity END`

func (q *QueryStore) SiteSummaries(ctx context.Context, site fleet.SiteID) ([]query.SiteSummary, error) {
	args := pgx.NamedArgs{"site": site, "offline_after": q.policy.Liveness.OfflineAfter, "low": q.policy.Battery.LowBelow}
	rows, err := q.db.pool.Query(ctx, `
		SELECT id, name FROM sites WHERE @site = '' OR id = @site ORDER BY id`, args)
	if err != nil {
		return nil, err
	}
	sites, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (query.SiteSummary, error) {
		s := query.SiteSummary{Connectivity: map[fleet.Connectivity]int{}, Modes: map[fleet.Mode]int{}}
		return s, r.Scan(&s.SiteID, &s.Name)
	})
	if err != nil {
		return nil, err
	}
	idx := make(map[fleet.SiteID]*query.SiteSummary, len(sites))
	for i := range sites {
		idx[sites[i].SiteID] = &sites[i]
	}

	// One grouped scan over robots and their state, assembled in Go.
	rows, err = q.db.pool.Query(ctx, `
		SELECT r.site_id, `+effectiveConnectivity+` AS conn, coalesce(s.mode, ''), count(*),
			count(*) FILTER (WHERE s.battery_pct < @low)
		FROM robots r LEFT JOIN robot_state s ON s.robot_id = r.id
		WHERE @site = '' OR r.site_id = @site
		GROUP BY 1, 2, 3`, args)
	if err != nil {
		return nil, err
	}
	var (
		siteID     fleet.SiteID
		conn       fleet.Connectivity
		mode       fleet.Mode
		count, low int
	)
	_, err = pgx.ForEachRow(rows, []any{&siteID, &conn, &mode, &count, &low}, func() error {
		s, ok := idx[siteID]
		if !ok {
			return nil
		}
		s.Robots += count
		s.Connectivity[conn] += count
		s.BatteryLow += low
		// The mode of a robot that is not reporting is history, not "now".
		if conn == fleet.Online || conn == fleet.Stale {
			s.Modes[mode] += count
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	rows, err = q.db.pool.Query(ctx, `
		SELECT site_id, count(*), count(*) FILTER (WHERE severity = 'critical')
		FROM alerts
		WHERE cleared_at IS NULL AND (@site = '' OR site_id = @site)
		GROUP BY 1`, args)
	if err != nil {
		return nil, err
	}
	var active, critical int
	_, err = pgx.ForEachRow(rows, []any{&siteID, &active, &critical}, func() error {
		if s, ok := idx[siteID]; ok {
			s.ActiveAlerts, s.Critical = active, critical
		}
		return nil
	})
	return sites, err
}

const robotSelect = `
	SELECT r.id, r.site_id, r.name, r.model, r.manufacturer, r.serial_number,
		` + effectiveConnectivity + ` AS conn,
		s.last_seen_at, s.device_time, s.received_at, s.mode, s.battery_pct, s.charging,
		s.map_id, s.x, s.y, s.heading, s.faults,
		(SELECT count(*) FROM alerts a WHERE a.robot_id = r.id AND a.cleared_at IS NULL) AS active_alerts
	FROM robots r LEFT JOIN robot_state s ON s.robot_id = r.id`

// ListRobots returns the robots that need attention first: most active
// alerts, then by ID so the order is stable.
func (q *QueryStore) ListRobots(ctx context.Context, f query.RobotFilter) ([]query.Robot, error) {
	rows, err := q.db.pool.Query(ctx, robotSelect+`
		WHERE (@site = '' OR r.site_id = @site)
			AND (@conn = '' OR `+effectiveConnectivity+` = @conn)
		ORDER BY active_alerts DESC, r.id
		LIMIT @limit`,
		pgx.NamedArgs{"site": f.SiteID, "conn": f.Connectivity, "limit": f.Limit,
			"offline_after": q.policy.Liveness.OfflineAfter})
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanRobot)
}

func (q *QueryStore) GetRobot(ctx context.Context, id fleet.RobotID) (query.Robot, error) {
	rows, err := q.db.pool.Query(ctx, robotSelect+` WHERE r.id = @id`,
		pgx.NamedArgs{"id": id, "offline_after": q.policy.Liveness.OfflineAfter})
	if err != nil {
		return query.Robot{}, err
	}
	r, err := pgx.CollectExactlyOneRow(rows, scanRobot)
	if errors.Is(err, pgx.ErrNoRows) {
		return query.Robot{}, fmt.Errorf("robot %s: %w", id, query.ErrNotFound)
	}
	return r, err
}

func scanRobot(row pgx.CollectableRow) (query.Robot, error) {
	var (
		r                       query.Robot
		lastSeen, devTime, recv *time.Time
		mode, mapID             *string
		battery                 *float32
		charging                *bool
		x, y, heading           *float64
		faults                  []byte
	)
	err := row.Scan(&r.ID, &r.SiteID, &r.Name, &r.Model, &r.Manufacturer, &r.Serial, &r.Connectivity,
		&lastSeen, &devTime, &recv, &mode, &battery, &charging, &mapID, &x, &y, &heading, &faults, &r.ActiveAlerts)
	if err != nil || lastSeen == nil {
		return r, err
	}
	var fj []faultJSON
	if err := json.Unmarshal(faults, &fj); err != nil {
		return r, err
	}
	r.State = &query.State{
		LastSeen: *lastSeen, DeviceTime: *devTime, ReceivedAt: *recv,
		Mode:    fleet.Mode(*mode),
		Battery: fleet.Battery{Percent: float64(*battery), Charging: *charging},
		Pose:    fleet.Pose{MapID: *mapID, X: *x, Y: *y, Heading: *heading},
		Faults:  fromFaultsJSON(fj),
	}
	return r, nil
}

func (q *QueryStore) ListAlerts(ctx context.Context, f query.AlertFilter) ([]query.Alert, error) {
	rows, err := q.db.pool.Query(ctx, `
		SELECT a.id, a.robot_id, r.name, a.site_id, a.kind, a.code, a.severity, a.message, a.raised_at, a.cleared_at
		FROM alerts a JOIN robots r ON r.id = a.robot_id
		WHERE (@site = '' OR a.site_id = @site)
			AND (@robot = '' OR a.robot_id = @robot)
			AND (NOT @active_only OR a.cleared_at IS NULL)
		ORDER BY a.raised_at DESC, a.id DESC
		LIMIT @limit`,
		pgx.NamedArgs{"site": f.SiteID, "robot": f.RobotID, "active_only": f.ActiveOnly, "limit": f.Limit})
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (query.Alert, error) {
		var al query.Alert
		err := row.Scan(&al.ID, &al.RobotID, &al.RobotName, &al.SiteID, &al.Kind, &al.Code, &al.Severity,
			&al.Message, &al.RaisedAt, &al.ClearedAt)
		return al, err
	})
}

// Telemetry buckets a robot's history. Battery is averaged and its minimum
// kept, because a dip is what a fleet manager looks for; mode and charging
// are taken from the last sample in each bucket.
func (q *QueryStore) Telemetry(ctx context.Context, id fleet.RobotID, from, to time.Time, bucket time.Duration) ([]query.Point, error) {
	rows, err := q.db.pool.Query(ctx, `
		SELECT date_bin(@bucket::interval, received_at, @from) AS t,
			avg(battery_pct)::float8, min(battery_pct)::float8,
			(array_agg(charging ORDER BY received_at DESC))[1],
			(array_agg(mode ORDER BY received_at DESC))[1],
			count(*)
		FROM telemetry
		WHERE robot_id = @id AND received_at >= @from AND received_at < @to
		GROUP BY 1 ORDER BY 1`,
		pgx.NamedArgs{"id": id, "from": from, "to": to, "bucket": bucket})
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (query.Point, error) {
		var p query.Point
		err := row.Scan(&p.At, &p.BatteryAvg, &p.BatteryMin, &p.Charging, &p.Mode, &p.Samples)
		return p, err
	})
}
