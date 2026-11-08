package fleet

import (
	"maps"
	"slices"
	"time"
)

// Snapshot is a read-only copy of a robot's current state, taken so it can
// be persisted or published without sharing the Robot across goroutines.
type Snapshot struct {
	RobotID      RobotID
	SiteID       SiteID
	Last         Sample
	HasSample    bool
	LastSeen     time.Time
	Connectivity Connectivity
	Disconnected bool
	Alerts       []Alert
}

// ClockSkew is how far the robot's clock is behind the server's on its last
// report, transport delay included. Useful to spot robots with a broken clock.
func (s Snapshot) ClockSkew() time.Duration {
	if !s.HasSample {
		return 0
	}
	return s.Last.ReceivedAt.Sub(s.Last.DeviceTime)
}

// Snapshot copies the robot's state.
func (r *Robot) Snapshot() Snapshot {
	last := r.last
	last.Faults = slices.Clone(r.last.Faults)
	alerts := slices.Collect(maps.Values(r.alerts))
	slices.SortFunc(alerts, func(a, b Alert) int { return compareKeys(a.Key, b.Key) })
	return Snapshot{
		RobotID:      r.ID,
		SiteID:       r.SiteID,
		Last:         last,
		HasSample:    r.hasSample,
		LastSeen:     r.lastSeen,
		Connectivity: r.connectivity,
		Disconnected: r.disconnected,
		Alerts:       alerts,
	}
}

// Restore rebuilds a robot from persisted state after a restart, so active
// alerts are not raised a second time and ordering continues where it left
// off.
//
// notBefore is the time the process started. Silence is measured from it,
// not from the last report: while the process was down nobody was listening,
// so the outage says nothing about the robots. Without this, every robot
// would be declared offline the moment the service came back. The true
// last-seen time is kept for display.
func Restore(s Snapshot, notBefore time.Time) *Robot {
	r := NewRobot(s.RobotID, s.SiteID)
	r.last = s.Last
	r.hasSample = s.HasSample
	r.lastSeen = s.LastSeen
	r.silentSince = s.LastSeen
	if r.hasSample && r.silentSince.Before(notBefore) {
		r.silentSince = notBefore
	}
	r.connectivity = s.Connectivity
	if r.connectivity == "" {
		r.connectivity = Offline
	}
	r.disconnected = s.Disconnected
	for _, a := range s.Alerts {
		r.alerts[a.Key] = a
	}
	return r
}
