package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/sudopower/olivaw-fleet/internal/fleet"
	"github.com/sudopower/olivaw-fleet/internal/ingest"
	"github.com/sudopower/olivaw-fleet/internal/query"
)

func TestListRobotsPutsAlertingRobotsFirst(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	seed(t, db, "r1", "r2", "r3")
	err := db.State().WriteBatch(ctx, ingest.Batch{Events: []fleet.Event{
		alertEvent(fleet.EventAlertRaised, "r3", fleet.AlertBatteryLow, t0),
	}})
	if err != nil {
		t.Fatal(err)
	}
	robots, err := db.Query(fleet.DefaultPolicy()).ListRobots(ctx, query.RobotFilter{SiteID: "site-a", Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(robots) != 2 || robots[0].ID != "r3" || robots[0].ActiveAlerts != 1 || robots[1].ID != "r1" {
		t.Fatalf("robots = %+v", robots)
	}
}

func TestEffectiveConnectivity(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	seed(t, db, "fresh", "silent", "never")
	now := time.Now()
	err := db.State().WriteBatch(ctx, ingest.Batch{Snapshots: []fleet.Snapshot{
		snapshot("fresh", now, 80, fleet.Online),
		// Stored as online, but nothing has updated it for an hour: the
		// pipeline must have stopped. Reads must not claim it is online.
		snapshot("silent", now.Add(-time.Hour), 80, fleet.Online),
	}})
	if err != nil {
		t.Fatal(err)
	}
	q := db.Query(fleet.DefaultPolicy())
	want := map[fleet.RobotID]fleet.Connectivity{"fresh": fleet.Online, "silent": fleet.Offline, "never": query.NeverSeen}
	for id, conn := range want {
		r, err := q.GetRobot(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if r.Connectivity != conn {
			t.Errorf("%s: connectivity = %s, want %s", id, r.Connectivity, conn)
		}
		if (id == "never") != (r.State == nil) {
			t.Errorf("%s: state = %+v", id, r.State)
		}
	}

	offline, err := q.ListRobots(ctx, query.RobotFilter{Connectivity: fleet.Offline, Limit: 10})
	if err != nil || len(offline) != 1 || offline[0].ID != "silent" {
		t.Fatalf("offline filter = %+v, %v", offline, err)
	}
	if _, err := q.GetRobot(ctx, "nope"); err == nil {
		t.Fatal("want not found")
	}
}

func TestSiteSummaries(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	seed(t, db, "r1", "r2", "r3")
	now := time.Now()
	err := db.State().WriteBatch(ctx, ingest.Batch{
		Snapshots: []fleet.Snapshot{snapshot("r1", now, 15, fleet.Online), snapshot("r2", now, 90, fleet.Online)},
		Events: []fleet.Event{
			alertEvent(fleet.EventAlertRaised, "r1", fleet.AlertBatteryLow, now),
			{Type: fleet.EventAlertRaised, RobotID: "r2", SiteID: "site-a", At: now,
				Alert: fleet.Alert{Key: fleet.AlertKey{Kind: fleet.AlertFault, Code: "E9"}, Severity: fleet.SeverityCritical, RaisedAt: now}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	sums, err := db.Query(fleet.DefaultPolicy()).SiteSummaries(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(sums) != 1 {
		t.Fatalf("sites = %d", len(sums))
	}
	s := sums[0]
	if s.Robots != 3 || s.Connectivity[fleet.Online] != 2 || s.Connectivity[query.NeverSeen] != 1 ||
		s.Modes[fleet.ModeCharging] != 2 || s.BatteryLow != 1 || s.ActiveAlerts != 2 || s.Critical != 1 {
		t.Fatalf("summary = %+v", s)
	}
}

func TestAlertsAndTelemetryBuckets(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	seed(t, db, "r1")
	st := db.State()

	var history []fleet.Sample
	for i := range 12 { // one sample every 5 s for a minute, battery falling
		s := snapshot("r1", t0.Add(time.Duration(i)*5*time.Second), float64(60-i), fleet.Online).Last
		history = append(history, s)
	}
	events := []fleet.Event{
		alertEvent(fleet.EventAlertRaised, "r1", fleet.AlertBatteryLow, t0),
		alertEvent(fleet.EventAlertRaised, "r1", fleet.AlertOffline, t0.Add(time.Second)),
		alertEvent(fleet.EventAlertRaised, "r1", fleet.AlertBatteryCritical, t0.Add(2*time.Second)),
	}
	if err := st.WriteBatch(ctx, ingest.Batch{History: history, Events: events}); err != nil {
		t.Fatal(err)
	}
	q := db.Query(fleet.DefaultPolicy())

	alerts, err := q.ListAlerts(ctx, query.AlertFilter{RobotID: "r1", ActiveOnly: true, Limit: 2})
	if err != nil || len(alerts) != 2 || alerts[0].Kind != fleet.AlertBatteryCritical || alerts[1].Kind != fleet.AlertOffline {
		t.Fatalf("alerts = %+v, %v (want newest first, limited)", alerts, err)
	}

	points, err := q.Telemetry(ctx, "r1", t0, t0.Add(time.Minute), 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(points) != 2 || points[0].Samples != 6 || points[0].BatteryMin != 55 || points[0].BatteryAvg != 57.5 {
		t.Fatalf("points = %+v", points)
	}
}
