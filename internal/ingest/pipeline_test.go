package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/sudopower/olivaw-fleet/internal/fleet"
	"github.com/sudopower/olivaw-fleet/internal/registry"
	"github.com/sudopower/olivaw-fleet/internal/telemetry"
)

var t0 = time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)

type fakeResolver map[telemetry.Device]registry.Robot

func (f fakeResolver) Resolve(d telemetry.Device) (registry.Robot, bool) {
	r, ok := f[d]
	return r, ok
}

func resolver(serials ...string) fakeResolver {
	f := fakeResolver{}
	for _, s := range serials {
		d := telemetry.Device{Manufacturer: "acme", Serial: s}
		f[d] = registry.Robot{ID: fleet.RobotID("rbt_" + s), SiteID: "site-a", Device: d}
	}
	return f
}

// fakeStore records batches and can be told to fail.
type fakeStore struct {
	mu       sync.Mutex
	batches  []Batch
	failNext int
	snaps    []fleet.Snapshot
}

func (s *fakeStore) WriteBatch(_ context.Context, b Batch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failNext > 0 {
		s.failNext--
		return errors.New("database unavailable")
	}
	s.batches = append(s.batches, b)
	return nil
}

func (s *fakeStore) LoadSnapshots(context.Context) ([]fleet.Snapshot, error) { return s.snaps, nil }

func (s *fakeStore) written() (snaps []fleet.Snapshot, history []fleet.Sample, events []fleet.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, b := range s.batches {
		snaps = append(snaps, b.Snapshots...)
		history = append(history, b.History...)
		events = append(events, b.Events...)
	}
	return
}

// state builds a VDA 5050 state message sent sec seconds after t0, with a
// battery of pct percent.
func state(serial string, sec float64, pct float64) (string, []byte, time.Time) {
	at := t0.Add(time.Duration(sec * float64(time.Second)))
	p, _ := json.Marshal(telemetry.VDA5050State{
		Timestamp: at, Manufacturer: "acme", SerialNumber: serial, OperatingMode: "AUTOMATIC", Driving: true,
		BatteryState: telemetry.VDA5050Battery{BatteryCharge: pct},
	})
	return "uagv/v2/acme/" + serial + "/state", p, at
}

func newTestPipeline(cfg Config, res Resolver, store Store) (*Pipeline, *Metrics) {
	m := NewMetrics(prometheus.NewRegistry())
	p := New(cfg, telemetry.NewRouter(telemetry.VDA5050{}), res, store, m, slog.New(slog.NewTextHandler(io.Discard, nil)))
	return p, m
}

func testConfig() Config {
	c := DefaultConfig()
	c.FlushInterval = 10 * time.Millisecond
	c.TickInterval = time.Hour // liveness is driven explicitly in tests
	return c
}

// feed handles messages synchronously, without Run.
func (p *Pipeline) feed(topic string, payload []byte, at time.Time) {
	p.Handle(topic, payload, at)
	p.handle(<-p.in)
}

// flushed runs one flush and returns what was written.
func flushed(p *Pipeline, store *fakeStore) Batch {
	n := len(store.batches)
	p.flush(context.Background())
	if len(store.batches) == n {
		return Batch{}
	}
	return store.batches[n]
}

func TestCoalescesStateAndDecimatesHistory(t *testing.T) {
	store := &fakeStore{}
	p, _ := newTestPipeline(testConfig(), resolver("SN-1"), store)

	// 12 seconds of samples at 1 Hz with a 5 s history interval.
	for i := range 12 {
		p.feed(state("SN-1", float64(i), 90))
	}
	b := flushed(p, store)

	if len(b.Snapshots) != 1 {
		t.Fatalf("snapshots = %d, want 1 (coalesced)", len(b.Snapshots))
	}
	if got := b.Snapshots[0].Last.DeviceTime; !got.Equal(t0.Add(11 * time.Second)) {
		t.Fatalf("snapshot is not the latest sample: %v", got)
	}
	// Kept: t=0 (first, caused events), t=5, t=10.
	if len(b.History) != 3 {
		t.Fatalf("history = %d rows, want 3", len(b.History))
	}
	if b.History[0].RobotID != "rbt_SN-1" {
		t.Fatalf("history row has robot id %q", b.History[0].RobotID)
	}
}

func TestKeepsHistoryForSamplesThatCauseEvents(t *testing.T) {
	store := &fakeStore{}
	p, _ := newTestPipeline(testConfig(), resolver("SN-1"), store)
	for i, pct := range []float64{50, 50, 15} { // third sample raises battery_low
		p.feed(state("SN-1", float64(i), pct))
	}
	b := flushed(p, store)
	if len(b.History) != 2 {
		t.Fatalf("history = %d rows, want 2 (first sample and the alerting one)", len(b.History))
	}
}

func TestPipelineEndToEnd(t *testing.T) {
	store := &fakeStore{}
	p, m := newTestPipeline(testConfig(), resolver("SN-1", "SN-2"), store)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { p.Run(ctx); close(done) }()

	for i := range 5 {
		for _, sn := range []string{"SN-1", "SN-2"} {
			p.Handle(state(sn, float64(i), 80))
		}
	}
	p.Handle(state("SN-1", 4, 80))                                  // duplicate
	p.Handle(state("SN-9", 0, 80))                                  // not provisioned
	p.Handle("uagv/v2/acme/SN-1/state", []byte(`{"headerId":`), t0) // malformed
	p.Handle("other/topic", nil, t0)                                // unrouted
	p.Handle(state("SN-2", 5, 170))                                 // battery 170%: invalid
	waitFor(t, func() bool { return totalMessages(m) == 15 })
	cancel()
	<-done

	snaps, _, events := store.written()
	latest := map[fleet.RobotID]fleet.Snapshot{}
	for _, s := range snaps {
		latest[s.RobotID] = s
	}
	if len(latest) != 2 {
		t.Fatalf("robots persisted = %d", len(latest))
	}
	for id, s := range latest {
		if !s.Last.DeviceTime.Equal(t0.Add(4 * time.Second)) {
			t.Errorf("%s: last device time %v", id, s.Last.DeviceTime)
		}
	}
	if len(events) != 2 { // each robot came online once
		t.Errorf("events = %d, want 2", len(events))
	}

	for result, want := range map[string]float64{
		resultAccepted: 10, resultDuplicate: 1, resultUnknownRobot: 1,
		resultMalformed: 1, resultUnrouted: 1, resultInvalid: 1,
	} {
		if got := testutil.ToFloat64(m.Messages.WithLabelValues(result)); got != want {
			t.Errorf("%s = %v, want %v", result, got, want)
		}
	}
}

func TestFailedWritesAreRetriedWithoutLosingEvents(t *testing.T) {
	store := &fakeStore{failNext: 3}
	p, m := newTestPipeline(testConfig(), resolver("SN-1"), store)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { p.Run(ctx); close(done) }()

	p.Handle(state("SN-1", 0, 50)) // online
	p.Handle(state("SN-1", 1, 15)) // battery_low raised
	p.Handle(state("SN-1", 2, 50)) // battery_low cleared

	waitFor(t, func() bool { _, _, ev := store.written(); return len(ev) == 3 })
	cancel()
	<-done

	if got := testutil.ToFloat64(m.FlushErrors); got < 3 {
		t.Fatalf("flush errors = %v, want >= 3", got)
	}
	_, _, events := store.written()
	want := []fleet.EventType{fleet.EventConnectivityChanged, fleet.EventAlertRaised, fleet.EventAlertCleared}
	for i, e := range events {
		if e.Type != want[i] {
			t.Fatalf("event %d = %v, want %v (order must survive retries)", i, e.Type, want[i])
		}
	}
}

func TestBackpressureDropsSamplesNotConnectionEvents(t *testing.T) {
	cfg := testConfig()
	cfg.Buffer = 1
	p, m := newTestPipeline(cfg, resolver("SN-1"), &fakeStore{})
	// Run is not running, so the inbox fills up.
	p.Handle(state("SN-1", 0, 50))
	p.Handle(state("SN-1", 1, 50))
	p.Handle(state("SN-1", 2, 50))
	if got := testutil.ToFloat64(m.Messages.WithLabelValues(resultDropped)); got != 2 {
		t.Fatalf("dropped = %v, want 2", got)
	}
}

func TestRestoreResumesState(t *testing.T) {
	cfg := testConfig()
	store := &fakeStore{snaps: []fleet.Snapshot{{
		RobotID: "rbt_SN-1", SiteID: "site-a", HasSample: true, Connectivity: fleet.Online,
		LastSeen: t0, Last: fleet.Sample{RobotID: "rbt_SN-1", DeviceTime: t0, ReceivedAt: t0, Battery: fleet.Battery{Percent: 15}},
		Alerts: []fleet.Alert{{Key: fleet.AlertKey{Kind: fleet.AlertBatteryLow}, Severity: fleet.SeverityWarning, RaisedAt: t0}},
	}}}
	p, _ := newTestPipeline(cfg, resolver("SN-1"), store)
	if err := p.Restore(context.Background(), t0.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	p.feed(state("SN-1", 61, 15))
	if b := flushed(p, store); len(b.Events) != 0 {
		t.Fatalf("events after restore = %+v, want none (already online, alert already active)", b.Events)
	}
}

func totalMessages(m *Metrics) float64 {
	var sum float64
	for _, c := range m.results {
		sum += testutil.ToFloat64(c)
	}
	return sum
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in time")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
