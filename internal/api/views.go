package api

import (
	"math"
	"time"

	"github.com/sudopower/olivaw-fleet/internal/fleet"
	"github.com/sudopower/olivaw-fleet/internal/query"
)

// Wire shapes. Field names are snake_case and stable; internal types can
// change without breaking clients.

type summaryView struct {
	Robots       int            `json:"robots"`
	Connectivity map[string]int `json:"connectivity"`
	Modes        map[string]int `json:"modes"`
	BatteryLow   int            `json:"battery_low"`
	ActiveAlerts int            `json:"active_alerts"`
	Critical     int            `json:"critical_alerts"`
}

type siteView struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	summaryView
}

func newSummary() summaryView {
	s := summaryView{Connectivity: map[string]int{}, Modes: map[string]int{}}
	// Always present, so clients can render zeros without special cases.
	for _, c := range []fleet.Connectivity{fleet.Online, fleet.Stale, fleet.Offline, query.NeverSeen} {
		s.Connectivity[string(c)] = 0
	}
	return s
}

func (s *summaryView) add(q query.SiteSummary) {
	s.Robots += q.Robots
	for k, v := range q.Connectivity {
		s.Connectivity[string(k)] += v
	}
	for k, v := range q.Modes {
		s.Modes[string(k)] += v
	}
	s.BatteryLow += q.BatteryLow
	s.ActiveAlerts += q.ActiveAlerts
	s.Critical += q.Critical
}

type batteryView struct {
	Percent  float64 `json:"percent"`
	Charging bool    `json:"charging"`
}

type poseView struct {
	MapID   string  `json:"map_id"`
	X       float64 `json:"x"`
	Y       float64 `json:"y"`
	Heading float64 `json:"heading"`
}

type faultView struct {
	Code     string `json:"code"`
	Severity string `json:"severity"`
	Message  string `json:"message,omitempty"`
}

type stateView struct {
	LastSeenAt  time.Time   `json:"last_seen_at"`
	DeviceTime  time.Time   `json:"device_time"`
	ClockSkewMs int64       `json:"clock_skew_ms"`
	Mode        string      `json:"mode"`
	Battery     batteryView `json:"battery"`
	Pose        poseView    `json:"pose"`
	Faults      []faultView `json:"faults"`
}

type robotView struct {
	ID           string     `json:"id"`
	SiteID       string     `json:"site_id"`
	Name         string     `json:"name"`
	Model        string     `json:"model"`
	Manufacturer string     `json:"manufacturer"`
	Serial       string     `json:"serial"`
	Connectivity string     `json:"connectivity"`
	ActiveAlerts int        `json:"active_alerts"`
	State        *stateView `json:"state"` // null until the robot first reports
}

func toState(lastSeen time.Time, s fleet.Sample) *stateView {
	v := &stateView{
		LastSeenAt:  lastSeen,
		DeviceTime:  s.DeviceTime,
		ClockSkewMs: s.ReceivedAt.Sub(s.DeviceTime).Milliseconds(),
		Mode:        string(s.Mode),
		Battery:     batteryView{Percent: round1(s.Battery.Percent), Charging: s.Battery.Charging},
		Pose:        poseView{MapID: s.Pose.MapID, X: s.Pose.X, Y: s.Pose.Y, Heading: s.Pose.Heading},
		Faults:      make([]faultView, 0, len(s.Faults)),
	}
	for _, f := range s.Faults {
		v.Faults = append(v.Faults, faultView{Code: f.Code, Severity: string(f.Severity), Message: f.Message})
	}
	return v
}

func toRobot(r query.Robot) robotView {
	v := robotView{
		ID: string(r.ID), SiteID: string(r.SiteID), Name: r.Name, Model: r.Model,
		Manufacturer: r.Manufacturer, Serial: r.Serial,
		Connectivity: string(r.Connectivity), ActiveAlerts: r.ActiveAlerts,
	}
	if st := r.State; st != nil {
		v.State = toState(st.LastSeen, fleet.Sample{
			DeviceTime: st.DeviceTime, ReceivedAt: st.ReceivedAt, Mode: st.Mode,
			Battery: st.Battery, Pose: st.Pose, Faults: st.Faults,
		})
	}
	return v
}

type alertView struct {
	ID        int64      `json:"id"`
	RobotID   string     `json:"robot_id"`
	RobotName string     `json:"robot_name"`
	SiteID    string     `json:"site_id"`
	Kind      string     `json:"kind"`
	Code      string     `json:"code,omitempty"`
	Severity  string     `json:"severity"`
	Message   string     `json:"message"`
	RaisedAt  time.Time  `json:"raised_at"`
	ClearedAt *time.Time `json:"cleared_at"`
}

func toAlert(a query.Alert) alertView {
	return alertView{
		ID: a.ID, RobotID: string(a.RobotID), RobotName: a.RobotName, SiteID: string(a.SiteID),
		Kind: string(a.Kind), Code: a.Code, Severity: string(a.Severity), Message: a.Message,
		RaisedAt: a.RaisedAt, ClearedAt: a.ClearedAt,
	}
}

type pointView struct {
	At         time.Time `json:"at"`
	BatteryAvg float64   `json:"battery_avg"`
	BatteryMin float64   `json:"battery_min"`
	Charging   bool      `json:"charging"`
	Mode       string    `json:"mode"`
	Samples    int       `json:"samples"`
}

type list[T any] struct {
	Items []T `json:"items"`
}

func round1(v float64) float64 { return math.Round(v*10) / 10 }
