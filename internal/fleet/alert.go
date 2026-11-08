package fleet

import "time"

// AlertKind is a category of condition a fleet manager needs to act on.
type AlertKind string

const (
	AlertBatteryLow      AlertKind = "battery_low"
	AlertBatteryCritical AlertKind = "battery_critical"
	AlertOffline         AlertKind = "offline"
	AlertFault           AlertKind = "fault"
)

// AlertKey identifies an alert on one robot. Code is the vendor fault code for
// AlertFault and empty otherwise. At most one alert per key is active at a
// time; storage enforces the same invariant.
type AlertKey struct {
	Kind AlertKind
	Code string
}

// Alert is an active condition on a robot.
type Alert struct {
	Key      AlertKey
	Severity Severity
	Message  string
	RaisedAt time.Time // server time
}

// EventType names what changed.
type EventType string

const (
	EventAlertRaised         EventType = "alert_raised"
	EventAlertCleared        EventType = "alert_cleared"
	EventConnectivityChanged EventType = "connectivity_changed"
)

// Event is a state transition worth persisting and pushing to clients.
// Unlike samples, events are rare and must not be dropped: they are the
// record of what happened to the fleet.
type Event struct {
	Type    EventType
	RobotID RobotID
	SiteID  SiteID
	At      time.Time // server time

	// Set for alert events.
	Alert Alert

	// Set for connectivity events.
	Connectivity Connectivity
}
