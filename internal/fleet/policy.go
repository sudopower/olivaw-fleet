package fleet

import "time"

// Connectivity is the platform's judgement of whether a robot is reachable.
// Robots never report this themselves: a robot that has died cannot say so.
type Connectivity string

const (
	Online  Connectivity = "online"
	Stale   Connectivity = "stale"   // missed a few reports, may be a blip
	Offline Connectivity = "offline" // gone long enough to act on
)

// LivenessPolicy turns silence into a connectivity state.
type LivenessPolicy struct {
	StaleAfter   time.Duration
	OfflineAfter time.Duration
}

// Assess judges connectivity from the server time a robot was last heard from.
func (p LivenessPolicy) Assess(lastSeen, now time.Time) Connectivity {
	silent := now.Sub(lastSeen)
	switch {
	case silent >= p.OfflineAfter:
		return Offline
	case silent >= p.StaleAfter:
		return Stale
	default:
		return Online
	}
}

// BatteryPolicy sets the alert thresholds. Each alert clears at a higher
// level than it raises, so a battery hovering around a threshold does not
// raise and clear an alert on every report.
type BatteryPolicy struct {
	LowBelow        float64
	LowClearAt      float64
	CriticalBelow   float64
	CriticalClearAt float64
}

// Policy is every tunable rule in the domain.
type Policy struct {
	Liveness LivenessPolicy
	Battery  BatteryPolicy

	// ResyncAfter bounds how long an out-of-order sample is treated as late
	// delivery. If a sample with an older device time arrives after this much
	// server time has passed, the robot's clock has moved backwards (reboot,
	// NTP correction) and the sample is accepted as the new baseline.
	// Without this, a robot whose clock jumps back would be ignored forever.
	ResyncAfter time.Duration
}

// DefaultPolicy assumes robots report about once per second.
func DefaultPolicy() Policy {
	return Policy{
		Liveness: LivenessPolicy{
			StaleAfter:   5 * time.Second,
			OfflineAfter: 30 * time.Second,
		},
		Battery: BatteryPolicy{
			LowBelow:        20,
			LowClearAt:      25,
			CriticalBelow:   10,
			CriticalClearAt: 15,
		},
		ResyncAfter: 10 * time.Second,
	}
}
