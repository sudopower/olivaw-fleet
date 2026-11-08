// Package fleet is the domain model: what a robot reports, what the platform
// derives from it, and the rules that turn telemetry into things a fleet
// manager acts on. It does no IO and has no dependencies on the rest of the
// module, so every rule here is testable with plain values.
package fleet

import (
	"errors"
	"fmt"
	"math"
	"time"
)

// RobotID is the platform's identifier for a robot. It is independent of the
// vendor's own serial number, which only adapters and the registry know about.
type RobotID string

// SiteID identifies the building or location a robot is deployed at.
type SiteID string

// Mode is what the robot says it is doing, normalized across vendors.
type Mode string

const (
	ModeIdle     Mode = "idle"
	ModeWorking  Mode = "working"
	ModeCharging Mode = "charging"
	ModeManual   Mode = "manual"
	ModeError    Mode = "error"
	ModeUnknown  Mode = "unknown"
)

// Severity ranks faults and alerts.
type Severity string

const (
	SeverityWarning  Severity = "warning"
	SeverityCritical Severity = "critical"
)

// Fault is an error condition the robot reports about itself. Robots report
// the full set of active faults in every sample, so a fault that is no longer
// reported has been cleared.
type Fault struct {
	Code     string
	Severity Severity
	Message  string
}

// Pose is where the robot is on a site map.
type Pose struct {
	MapID   string
	X, Y    float64 // metres in the map frame
	Heading float64 // radians
}

// Battery is the robot's charge state.
type Battery struct {
	Percent  float64
	Charging bool
}

// Sample is one vendor-neutral telemetry report.
//
// Every sample carries the robot's complete state, not a delta. That property
// is what lets the pipeline drop or coalesce samples under load: the next one
// carries everything the dropped one did.
type Sample struct {
	RobotID RobotID

	// DeviceTime is the robot's own clock. It orders samples from one robot
	// but is never compared across robots or with the server clock.
	DeviceTime time.Time

	// ReceivedAt is the server clock when the message arrived. Liveness is
	// always judged on this, because robot clocks drift and reset.
	ReceivedAt time.Time

	Battery Battery
	Pose    Pose
	Mode    Mode
	Faults  []Fault
}

// ErrInvalidSample is wrapped by every validation failure.
var ErrInvalidSample = errors.New("invalid sample")

// Validate rejects samples that would corrupt derived state. It checks
// physical plausibility, not vendor format; adapters handle the format.
func (s Sample) Validate() error {
	switch {
	case s.RobotID == "":
		return fmt.Errorf("%w: missing robot id", ErrInvalidSample)
	case s.DeviceTime.IsZero():
		return fmt.Errorf("%w: missing device time", ErrInvalidSample)
	case s.ReceivedAt.IsZero():
		return fmt.Errorf("%w: missing received time", ErrInvalidSample)
	case !finite(s.Battery.Percent) || s.Battery.Percent < 0 || s.Battery.Percent > 100:
		return fmt.Errorf("%w: battery %v%% out of range", ErrInvalidSample, s.Battery.Percent)
	case !finite(s.Pose.X) || !finite(s.Pose.Y) || !finite(s.Pose.Heading):
		return fmt.Errorf("%w: non-finite pose", ErrInvalidSample)
	}
	for _, f := range s.Faults {
		if f.Code == "" {
			return fmt.Errorf("%w: fault without code", ErrInvalidSample)
		}
	}
	return nil
}

func finite(f float64) bool { return !math.IsNaN(f) && !math.IsInf(f, 0) }
