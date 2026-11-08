// Package query is the read model: what the presentation layer can ask
// about the fleet. It is separate from the write path in ingest so each can
// be shaped for its own access pattern: ingest writes coalesced snapshots,
// queries read them by site and status.
package query

import (
	"context"
	"errors"
	"time"

	"github.com/sudopower/olivaw-fleet/internal/fleet"
)

// NeverSeen is the connectivity of a provisioned robot that has not
// reported yet. It exists only on the read side.
const NeverSeen fleet.Connectivity = "never_seen"

// ErrNotFound is returned for unknown robots.
var ErrNotFound = errors.New("not found")

// Store answers queries. Implemented by postgres.
type Store interface {
	SiteSummaries(ctx context.Context, site fleet.SiteID) ([]SiteSummary, error)
	ListRobots(ctx context.Context, f RobotFilter) ([]Robot, error)
	GetRobot(ctx context.Context, id fleet.RobotID) (Robot, error)
	ListAlerts(ctx context.Context, f AlertFilter) ([]Alert, error)
	Telemetry(ctx context.Context, id fleet.RobotID, from, to time.Time, bucket time.Duration) ([]Point, error)
}

// SiteSummary is the "how is this site doing" row of the fleet overview.
type SiteSummary struct {
	SiteID       fleet.SiteID
	Name         string
	Robots       int
	Connectivity map[fleet.Connectivity]int
	Modes        map[fleet.Mode]int
	BatteryLow   int // below the low threshold, charging or not
	ActiveAlerts int
	Critical     int // active alerts of critical severity
}

// Robot is a provisioned robot with its current state, if any.
type Robot struct {
	ID           fleet.RobotID
	SiteID       fleet.SiteID
	Name         string
	Model        string
	Manufacturer string
	Serial       string

	// Connectivity is as of read time: if the ingest pipeline itself has
	// stopped, a robot that has been silent long enough reads as offline even
	// though the stored state still says online.
	Connectivity fleet.Connectivity
	State        *State // nil if never seen
	ActiveAlerts int
}

// State is the robot's last reported state.
type State struct {
	LastSeen   time.Time
	DeviceTime time.Time
	ReceivedAt time.Time
	Mode       fleet.Mode
	Battery    fleet.Battery
	Pose       fleet.Pose
	Faults     []fleet.Fault
}

// RobotFilter selects robots. Empty fields match everything.
type RobotFilter struct {
	SiteID       fleet.SiteID
	Connectivity fleet.Connectivity
	Limit        int
}

// Alert is an alert with the robot it belongs to.
type Alert struct {
	ID        int64
	RobotID   fleet.RobotID
	RobotName string
	SiteID    fleet.SiteID
	Kind      fleet.AlertKind
	Code      string
	Severity  fleet.Severity
	Message   string
	RaisedAt  time.Time
	ClearedAt *time.Time
}

// AlertFilter selects alerts, newest first. Empty fields match everything.
type AlertFilter struct {
	SiteID     fleet.SiteID
	RobotID    fleet.RobotID
	ActiveOnly bool
	Limit      int
}

// Point is one bucket of a robot's history.
type Point struct {
	At         time.Time // bucket start
	BatteryAvg float64
	BatteryMin float64
	Charging   bool       // charging at the end of the bucket
	Mode       fleet.Mode // mode at the end of the bucket
	Samples    int
}
