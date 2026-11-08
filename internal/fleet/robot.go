package fleet

import (
	"cmp"
	"fmt"
	"maps"
	"slices"
	"time"
)

// Robot is the derived, current state of one robot. It is the aggregate the
// ingest pipeline keeps per robot: samples go in, events come out.
//
// Robot is not safe for concurrent use. The pipeline gives each robot to
// exactly one goroutine, which also keeps its samples in order.
type Robot struct {
	ID     RobotID
	SiteID SiteID

	last      Sample // last accepted sample
	hasSample bool

	lastSeen time.Time // server time of the last accepted sample

	// silentSince is when the platform started waiting for the next report.
	// Normally equal to lastSeen; after a restart it is the restart time, so
	// the service's own downtime does not count as the robot's silence. Kept
	// apart from lastSeen so "last seen" shown to people stays true.
	silentSince time.Time

	connectivity Connectivity
	disconnected bool // the broker told us the connection dropped

	alerts map[AlertKey]Alert
}

// NewRobot returns a robot that has never reported.
func NewRobot(id RobotID, site SiteID) *Robot {
	return &Robot{
		ID:           id,
		SiteID:       site,
		connectivity: Offline,
		alerts:       make(map[AlertKey]Alert),
	}
}

// Outcome says what Observe did with a sample.
type Outcome int

const (
	Accepted Outcome = iota
	Duplicate
	OutOfOrder
)

func (o Outcome) String() string {
	switch o {
	case Accepted:
		return "accepted"
	case Duplicate:
		return "duplicate"
	case OutOfOrder:
		return "out_of_order"
	}
	return fmt.Sprintf("outcome(%d)", int(o))
}

// Observe applies a validated sample and returns the events it caused.
//
// Ordering uses the robot's device time, because the transport may redeliver
// or reorder messages. A sample with the same device time as the last one is
// a duplicate; an older one is late and ignored, unless enough server time has
// passed that the robot's clock must have moved backwards (see
// Policy.ResyncAfter).
func (r *Robot) Observe(s Sample, p Policy) (Outcome, []Event) {
	if r.hasSample {
		switch {
		case s.DeviceTime.Equal(r.last.DeviceTime):
			return Duplicate, nil
		case s.DeviceTime.Before(r.last.DeviceTime) &&
			s.ReceivedAt.Sub(r.last.ReceivedAt) < p.ResyncAfter:
			return OutOfOrder, nil
		}
	}

	r.last = s
	r.hasSample = true
	if s.ReceivedAt.After(r.lastSeen) {
		r.lastSeen = s.ReceivedAt
	}
	if r.lastSeen.After(r.silentSince) {
		r.silentSince = r.lastSeen
	}
	r.disconnected = false

	at := s.ReceivedAt
	var events []Event
	events = r.setConnectivity(Online, at, events)
	events = r.evaluateBattery(s.Battery, at, p.Battery, events)
	events = r.evaluateFaults(s.Faults, at, events)
	return Accepted, events
}

// Disconnect records that the broker saw the robot's connection drop (an MQTT
// last-will message). This is faster than waiting for silence, so the robot
// goes offline at once. The next sample brings it back.
func (r *Robot) Disconnect(at time.Time) []Event {
	if !r.hasSample {
		return nil
	}
	r.disconnected = true
	return r.setConnectivity(Offline, at, nil)
}

// Tick re-judges connectivity at server time now. The pipeline calls it on a
// timer, because silence produces no samples to react to.
func (r *Robot) Tick(now time.Time, p Policy) []Event {
	if !r.hasSample {
		return nil // never reported; nothing to time out
	}
	c := p.Liveness.Assess(r.silentSince, now)
	if r.disconnected {
		c = Offline
	}
	return r.setConnectivity(c, now, nil)
}

func (r *Robot) setConnectivity(c Connectivity, at time.Time, events []Event) []Event {
	if c == r.connectivity {
		return events
	}
	r.connectivity = c
	events = append(events, Event{
		Type:         EventConnectivityChanged,
		RobotID:      r.ID,
		SiteID:       r.SiteID,
		At:           at,
		Connectivity: c,
	})

	offline := AlertKey{Kind: AlertOffline}
	switch c {
	case Offline:
		events = r.raise(Alert{Key: offline, Severity: SeverityCritical, Message: "robot is not reporting"}, at, events)
	case Online:
		events = r.clear(offline, at, events)
	}
	// Stale changes connectivity but neither raises nor clears: it is a
	// warning sign, not yet something to page anyone about.
	return events
}

// evaluateBattery raises battery alerts only when someone needs to act. A
// robot that is charging is already being handled, so charging clears its
// battery alerts and keeps new ones from being raised. A robot that runs low
// away from its dock, or whose charger stopped working, is what the alert is
// for.
func (r *Robot) evaluateBattery(b Battery, at time.Time, p BatteryPolicy, events []Event) []Event {
	events = r.hysteresis(AlertKey{Kind: AlertBatteryCritical}, b, p.CriticalBelow, p.CriticalClearAt,
		SeverityCritical, at, events)
	events = r.hysteresis(AlertKey{Kind: AlertBatteryLow}, b, p.LowBelow, p.LowClearAt,
		SeverityWarning, at, events)
	return events
}

// hysteresis raises key when the charge drops below raiseBelow and clears it
// only once it is back at or above clearAt, or the robot is charging.
func (r *Robot) hysteresis(key AlertKey, b Battery, raiseBelow, clearAt float64, sev Severity, at time.Time, events []Event) []Event {
	_, active := r.alerts[key]
	switch {
	case !active && !b.Charging && b.Percent < raiseBelow:
		return r.raise(Alert{Key: key, Severity: sev, Message: fmt.Sprintf("battery at %.0f%%", b.Percent)}, at, events)
	case active && (b.Charging || b.Percent >= clearAt):
		return r.clear(key, at, events)
	}
	return events
}

// evaluateFaults compares the reported fault set with the active fault
// alerts: new codes are raised, missing codes are cleared.
func (r *Robot) evaluateFaults(faults []Fault, at time.Time, events []Event) []Event {
	reported := make(map[AlertKey]bool, len(faults))
	for _, f := range faults {
		reported[AlertKey{Kind: AlertFault, Code: f.Code}] = true
	}
	// Sorted so the same input always yields events in the same order.
	for _, key := range slices.SortedFunc(maps.Keys(r.alerts), compareKeys) {
		if key.Kind == AlertFault && !reported[key] {
			events = r.clear(key, at, events)
		}
	}
	for _, f := range faults {
		key := AlertKey{Kind: AlertFault, Code: f.Code}
		events = r.raise(Alert{Key: key, Severity: f.Severity, Message: f.Message}, at, events)
	}
	return events
}

func compareKeys(a, b AlertKey) int {
	return cmp.Or(cmp.Compare(a.Kind, b.Kind), cmp.Compare(a.Code, b.Code))
}

func (r *Robot) raise(a Alert, at time.Time, events []Event) []Event {
	if _, active := r.alerts[a.Key]; active {
		return events
	}
	a.RaisedAt = at
	r.alerts[a.Key] = a
	return append(events, Event{Type: EventAlertRaised, RobotID: r.ID, SiteID: r.SiteID, At: at, Alert: a})
}

func (r *Robot) clear(key AlertKey, at time.Time, events []Event) []Event {
	a, active := r.alerts[key]
	if !active {
		return events
	}
	delete(r.alerts, key)
	return append(events, Event{Type: EventAlertCleared, RobotID: r.ID, SiteID: r.SiteID, At: at, Alert: a})
}
