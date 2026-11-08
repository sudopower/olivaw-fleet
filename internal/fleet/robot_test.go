package fleet

import (
	"errors"
	"math"
	"testing"
	"time"
)

var t0 = time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)

// sampleAt builds a healthy sample whose device and server clocks agree.
func sampleAt(sec int) Sample {
	at := t0.Add(time.Duration(sec) * time.Second)
	return Sample{
		RobotID:    "r1",
		DeviceTime: at,
		ReceivedAt: at,
		Battery:    Battery{Percent: 80},
		Mode:       ModeWorking,
	}
}

func withBattery(s Sample, pct float64) Sample { s.Battery.Percent = pct; return s }
func withFaults(s Sample, f ...Fault) Sample   { s.Faults = f; return s }

// kinds summarizes events as "type:detail" so tests read as a sequence.
func kinds(events []Event) []string {
	out := make([]string, 0, len(events))
	for _, e := range events {
		switch e.Type {
		case EventConnectivityChanged:
			out = append(out, string(e.Type)+":"+string(e.Connectivity))
		default:
			d := string(e.Alert.Key.Kind)
			if e.Alert.Key.Code != "" {
				d += "/" + e.Alert.Key.Code
			}
			out = append(out, string(e.Type)+":"+d)
		}
	}
	return out
}

func assertEvents(t *testing.T, got []Event, want ...string) {
	t.Helper()
	g := kinds(got)
	if len(g) != len(want) {
		t.Fatalf("events = %v, want %v", g, want)
	}
	for i := range g {
		if g[i] != want[i] {
			t.Fatalf("events = %v, want %v", g, want)
		}
	}
}

func TestFirstSampleBringsRobotOnline(t *testing.T) {
	r := NewRobot("r1", "site-a")
	out, ev := r.Observe(sampleAt(0), DefaultPolicy())
	if out != Accepted {
		t.Fatalf("outcome = %v", out)
	}
	assertEvents(t, ev, "connectivity_changed:online")
	if ev[0].SiteID != "site-a" {
		t.Fatalf("event site = %q", ev[0].SiteID)
	}
}

func TestOrdering(t *testing.T) {
	p := DefaultPolicy()

	t.Run("duplicate device time is dropped", func(t *testing.T) {
		r := NewRobot("r1", "s")
		r.Observe(sampleAt(10), p)
		dup := sampleAt(10)
		dup.ReceivedAt = dup.ReceivedAt.Add(time.Second) // redelivered later
		if out, ev := r.Observe(dup, p); out != Duplicate || len(ev) != 0 {
			t.Fatalf("got %v %v", out, kinds(ev))
		}
	})

	t.Run("late sample is dropped and does not roll state back", func(t *testing.T) {
		r := NewRobot("r1", "s")
		r.Observe(withBattery(sampleAt(10), 50), p)
		late := withBattery(sampleAt(9), 5) // would raise battery alerts if applied
		late.ReceivedAt = t0.Add(11 * time.Second)
		if out, ev := r.Observe(late, p); out != OutOfOrder || len(ev) != 0 {
			t.Fatalf("got %v %v", out, kinds(ev))
		}
		if got := r.Snapshot().Last.Battery.Percent; got != 50 {
			t.Fatalf("battery = %v, want 50", got)
		}
	})

	t.Run("clock moving backwards resyncs after a while", func(t *testing.T) {
		r := NewRobot("r1", "s")
		r.Observe(sampleAt(100), p)
		rebooted := sampleAt(0) // robot clock reset to an older time
		rebooted.ReceivedAt = t0.Add(100*time.Second + p.ResyncAfter)
		if out, _ := r.Observe(rebooted, p); out != Accepted {
			t.Fatalf("outcome = %v, want accepted after resync window", out)
		}
		// Subsequent samples are ordered against the new clock.
		next := sampleAt(1)
		next.ReceivedAt = rebooted.ReceivedAt.Add(time.Second)
		if out, _ := r.Observe(next, p); out != Accepted {
			t.Fatalf("outcome = %v", out)
		}
	})

	t.Run("skewed clock is fine as long as it is monotonic", func(t *testing.T) {
		r := NewRobot("r1", "s")
		for i := range 3 {
			s := sampleAt(i)
			s.DeviceTime = s.DeviceTime.Add(-time.Hour) // robot is an hour behind
			if out, _ := r.Observe(s, p); out != Accepted {
				t.Fatalf("sample %d: %v", i, out)
			}
		}
		if skew := r.Snapshot().ClockSkew(); skew != time.Hour {
			t.Fatalf("skew = %v", skew)
		}
	})
}

func TestLiveness(t *testing.T) {
	p := DefaultPolicy()
	r := NewRobot("r1", "s")
	r.Observe(sampleAt(0), p)

	assertEvents(t, r.Tick(t0.Add(4*time.Second), p))
	assertEvents(t, r.Tick(t0.Add(5*time.Second), p), "connectivity_changed:stale")
	assertEvents(t, r.Tick(t0.Add(6*time.Second), p)) // no repeat
	assertEvents(t, r.Tick(t0.Add(30*time.Second), p),
		"connectivity_changed:offline", "alert_raised:offline")

	_, ev := r.Observe(sampleAt(31), p)
	assertEvents(t, ev, "connectivity_changed:online", "alert_cleared:offline")
}

func TestStaleRecoveryRaisesNoAlert(t *testing.T) {
	p := DefaultPolicy()
	r := NewRobot("r1", "s")
	r.Observe(sampleAt(0), p)
	r.Tick(t0.Add(6*time.Second), p)
	_, ev := r.Observe(sampleAt(7), p)
	assertEvents(t, ev, "connectivity_changed:online")
}

func TestNeverSeenRobotDoesNotTimeOut(t *testing.T) {
	r := NewRobot("r1", "s")
	assertEvents(t, r.Tick(t0.Add(time.Hour), DefaultPolicy()))
}

func TestDisconnectIsImmediateAndSticky(t *testing.T) {
	p := DefaultPolicy()
	r := NewRobot("r1", "s")
	r.Observe(sampleAt(0), p)

	ev := r.Disconnect(t0.Add(time.Second))
	assertEvents(t, ev, "connectivity_changed:offline", "alert_raised:offline")

	// A tick inside the liveness window must not flip it back to online.
	assertEvents(t, r.Tick(t0.Add(2*time.Second), p))

	_, ev = r.Observe(sampleAt(3), p)
	assertEvents(t, ev, "connectivity_changed:online", "alert_cleared:offline")
}

func TestBatteryHysteresis(t *testing.T) {
	p := DefaultPolicy()
	r := NewRobot("r1", "s")
	r.Observe(withBattery(sampleAt(0), 30), p)

	steps := []struct {
		pct  float64
		want []string
	}{
		{19, []string{"alert_raised:battery_low"}},
		{21, nil}, // above raise level, below clear level: hold
		{19, nil}, // already active: no repeat
		{9, []string{"alert_raised:battery_critical"}},
		{12, nil},
		{15, []string{"alert_cleared:battery_critical"}},
		{24.9, nil},
		{25, []string{"alert_cleared:battery_low"}},
	}
	for i, st := range steps {
		_, ev := r.Observe(withBattery(sampleAt(i+1), st.pct), p)
		assertEvents(t, ev, st.want...)
	}
}

func TestBatteryAlertsIgnoreChargingRobots(t *testing.T) {
	p := DefaultPolicy()
	r := NewRobot("r1", "s")
	charging := func(sec int, pct float64, on bool) Sample {
		s := withBattery(sampleAt(sec), pct)
		s.Battery.Charging = on
		return s
	}
	r.Observe(charging(0, 30, false), p)

	// Reached the dock low: charging, so nobody needs to act.
	_, ev := r.Observe(charging(1, 9, true), p)
	assertEvents(t, ev)

	// Charger fault: still at 9% but no longer charging. Now it matters.
	_, ev = r.Observe(charging(2, 9, false), p)
	assertEvents(t, ev, "alert_raised:battery_critical", "alert_raised:battery_low")

	// Someone fixed the charger: alerts clear without waiting for the charge.
	_, ev = r.Observe(charging(3, 9, true), p)
	assertEvents(t, ev, "alert_cleared:battery_critical", "alert_cleared:battery_low")
}

func TestFaultLifecycle(t *testing.T) {
	p := DefaultPolicy()
	r := NewRobot("r1", "s")
	r.Observe(sampleAt(0), p)

	bumper := Fault{Code: "BUMPER", Severity: SeverityWarning, Message: "bumper pressed"}
	lidar := Fault{Code: "LIDAR", Severity: SeverityCritical, Message: "lidar timeout"}

	_, ev := r.Observe(withFaults(sampleAt(1), bumper), p)
	assertEvents(t, ev, "alert_raised:fault/BUMPER")

	_, ev = r.Observe(withFaults(sampleAt(2), bumper, lidar), p)
	assertEvents(t, ev, "alert_raised:fault/LIDAR")
	if ev[0].Alert.Severity != SeverityCritical {
		t.Fatalf("severity = %v", ev[0].Alert.Severity)
	}

	_, ev = r.Observe(withFaults(sampleAt(3), lidar), p)
	assertEvents(t, ev, "alert_cleared:fault/BUMPER")

	_, ev = r.Observe(sampleAt(4), p)
	assertEvents(t, ev, "alert_cleared:fault/LIDAR")
}

func TestRestoreDoesNotReraiseOrTimeOutDuringOutage(t *testing.T) {
	p := DefaultPolicy()
	r := NewRobot("r1", "s")
	r.Observe(withBattery(sampleAt(0), 15), p) // battery_low active
	snap := r.Snapshot()

	// The service was down for ten minutes.
	started := t0.Add(10 * time.Minute)
	restored := Restore(snap, started)

	assertEvents(t, restored.Tick(started.Add(time.Second), p))
	if got := restored.Snapshot().LastSeen; !got.Equal(t0) {
		t.Fatalf("last seen = %v, want the real %v: the restart must not show as a report", got, t0)
	}

	s := withBattery(sampleAt(601), 15)
	_, ev := restored.Observe(s, p)
	assertEvents(t, ev) // still low, still online: nothing new

	assertEvents(t, restored.Tick(s.ReceivedAt.Add(p.Liveness.OfflineAfter), p),
		"connectivity_changed:offline", "alert_raised:offline")
}

func TestSnapshotIsACopy(t *testing.T) {
	p := DefaultPolicy()
	r := NewRobot("r1", "s")
	faults := []Fault{{Code: "A", Severity: SeverityWarning}}
	r.Observe(withFaults(sampleAt(0), faults...), p)
	snap := r.Snapshot()
	snap.Last.Faults[0].Code = "mutated"
	if r.Snapshot().Last.Faults[0].Code != "A" {
		t.Fatal("snapshot shares memory with the robot")
	}
}

func TestValidate(t *testing.T) {
	ok := sampleAt(0)
	cases := map[string]func(*Sample){
		"missing robot":      func(s *Sample) { s.RobotID = "" },
		"missing time":       func(s *Sample) { s.DeviceTime = time.Time{} },
		"battery > 100":      func(s *Sample) { s.Battery.Percent = 101 },
		"battery < 0":        func(s *Sample) { s.Battery.Percent = -1 },
		"battery NaN":        func(s *Sample) { s.Battery.Percent = math.NaN() },
		"pose infinite":      func(s *Sample) { s.Pose.X = math.Inf(1) },
		"fault without code": func(s *Sample) { s.Faults = []Fault{{}} },
	}
	if err := ok.Validate(); err != nil {
		t.Fatalf("valid sample rejected: %v", err)
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			s := ok
			mutate(&s)
			if err := s.Validate(); !errors.Is(err, ErrInvalidSample) {
				t.Fatalf("err = %v", err)
			}
		})
	}
}
