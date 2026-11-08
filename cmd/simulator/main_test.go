package main

import (
	"math/rand/v2"
	"testing"
	"time"

	"github.com/sudopower/olivaw-fleet/internal/fleet"
	"github.com/sudopower/olivaw-fleet/internal/telemetry"
)

// The simulator and the adapter must agree on the wire format. Decoding
// what the simulator publishes with the real adapter checks both sides.
func TestMessagesDecodeWithRealAdapter(t *testing.T) {
	router := telemetry.NewRouter(telemetry.VDA5050{})
	now := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	r := &robot{maker: "acme", serial: "SN-1", site: "site-01", battery: 42.37,
		clock: -2 * time.Second, faultUntil: now.Add(time.Minute)}

	env, ok := router.Route(r.topic("state"), r.state(now), now)
	if !ok {
		t.Fatalf("topic %q not routed", r.topic("state"))
	}
	d, err := env.Decode()
	if err != nil {
		t.Fatal(err)
	}
	s := d.Sample
	s.RobotID = "r"
	if err := s.Validate(); err != nil {
		t.Fatal(err)
	}
	if !s.DeviceTime.Equal(now.Add(-2 * time.Second)) {
		t.Errorf("device time %v, want robot clock 2 s behind", s.DeviceTime)
	}
	if s.Mode != fleet.ModeError || len(s.Faults) != 1 || s.Battery.Percent != 42.3 {
		t.Errorf("sample %+v, want error mode, one fault, battery 42.3", s)
	}

	env, _ = router.Route(r.topic("connection"), r.connectionBroken(now), now)
	if d, err := env.Decode(); err != nil || !d.Disconnected {
		t.Fatalf("last-will decoded as %+v, %v", d, err)
	}
}

func TestBatteryCycleAlertsOnlyForLemons(t *testing.T) {
	s := &sim{chaos: chaos{}}
	for _, tc := range []struct {
		dockAt  float64
		wantLow bool
	}{{dockAt, false}, {lemonDockAt, true}} {
		r := &robot{battery: 60, dockAt: tc.dockAt}
		s.rng = rand.New(rand.NewPCG(1, 1))
		now := time.Unix(0, 0)
		lowest, charged := 100.0, false
		for range 1000 {
			now = now.Add(time.Second)
			r.step(now, time.Second, s.chaos, s.rng)
			if !r.charging {
				lowest = min(lowest, r.battery)
			}
			charged = charged || r.charging
		}
		if !charged {
			t.Errorf("dockAt %v: robot never charged", tc.dockAt)
		}
		// fleetd raises battery_low below 20% when not charging.
		if gotLow := lowest < 20; gotLow != tc.wantLow {
			t.Errorf("dockAt %v: lowest uncharged battery %.1f, want alert %v", tc.dockAt, lowest, tc.wantLow)
		}
	}
}
