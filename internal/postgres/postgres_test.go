package postgres

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	neturl "net/url"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/sudopower/olivaw-fleet/internal/fleet"
	"github.com/sudopower/olivaw-fleet/internal/ingest"
	"github.com/sudopower/olivaw-fleet/internal/registry"
	"github.com/sudopower/olivaw-fleet/internal/telemetry"
)

var t0 = time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)

// testDB creates a throwaway database, migrated, and drops it afterwards.
// Set FLEET_TEST_DATABASE_URL to run (`make test-integration` does).
func testDB(t *testing.T) *DB {
	t.Helper()
	url := os.Getenv("FLEET_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("FLEET_TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	var b [4]byte
	_, _ = rand.Read(b[:])
	name := "fleet_test_" + hex.EncodeToString(b[:])
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	u, err := neturl.Parse(url)
	if err != nil {
		t.Fatal(err)
	}
	u.Path = "/" + name
	db, err := Open(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		db.Close()
		_, _ = admin.Exec(ctx, "DROP DATABASE "+name)
		admin.Close(ctx)
	})
	if err := db.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	return db
}

func seed(t *testing.T, db *DB, robots ...string) {
	t.Helper()
	ctx := context.Background()
	reg := db.Registry()
	if err := reg.CreateSite(ctx, registry.Site{ID: "site-a", Name: "Site A"}); err != nil {
		t.Fatal(err)
	}
	for _, id := range robots {
		err := reg.CreateRobot(ctx, registry.Robot{ID: fleet.RobotID(id), SiteID: "site-a",
			Device: telemetry.Device{Manufacturer: "acme", Serial: id}, Name: id})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestMigrateIsIdempotent(t *testing.T) {
	db := testDB(t)
	if err := db.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestRegistryStore(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	seed(t, db, "r1")
	reg := db.Registry()

	if err := reg.CreateSite(ctx, registry.Site{ID: "site-a", Name: "again"}); !errors.Is(err, registry.ErrConflict) {
		t.Fatalf("duplicate site: %v", err)
	}
	dup := registry.Robot{ID: "r2", SiteID: "site-a", Device: telemetry.Device{Manufacturer: "acme", Serial: "r1"}, Name: "x"}
	if err := reg.CreateRobot(ctx, dup); !errors.Is(err, registry.ErrConflict) {
		t.Fatalf("duplicate device: %v", err)
	}
	orphan := registry.Robot{ID: "r3", SiteID: "nowhere", Device: telemetry.Device{Manufacturer: "acme", Serial: "r3"}, Name: "x"}
	if err := reg.CreateRobot(ctx, orphan); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("unknown site: %v", err)
	}
	robots, err := reg.ListRobots(ctx)
	if err != nil || len(robots) != 1 || robots[0].Device.Serial != "r1" {
		t.Fatalf("robots = %+v, %v", robots, err)
	}
}

func snapshot(id string, at time.Time, battery float64, conn fleet.Connectivity) fleet.Snapshot {
	return fleet.Snapshot{
		RobotID: fleet.RobotID(id), SiteID: "site-a", HasSample: true, LastSeen: at, Connectivity: conn,
		Last: fleet.Sample{
			RobotID: fleet.RobotID(id), DeviceTime: at.Add(-time.Second), ReceivedAt: at,
			Battery: fleet.Battery{Percent: battery, Charging: true}, Mode: fleet.ModeCharging,
			Pose:   fleet.Pose{MapID: "f1", X: 1.5, Y: -2, Heading: 0.5},
			Faults: []fleet.Fault{{Code: "E1", Severity: fleet.SeverityWarning, Message: "brush worn"}},
		},
	}
}

func alertEvent(typ fleet.EventType, id string, kind fleet.AlertKind, at time.Time) fleet.Event {
	return fleet.Event{Type: typ, RobotID: fleet.RobotID(id), SiteID: "site-a", At: at,
		Alert: fleet.Alert{Key: fleet.AlertKey{Kind: kind}, Severity: fleet.SeverityWarning, Message: "m", RaisedAt: at}}
}

func TestWriteBatchRoundTrip(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	seed(t, db, "r1", "r2")
	st := db.State()

	s1 := snapshot("r1", t0, 18.5, fleet.Online)
	err := st.WriteBatch(ctx, ingest.Batch{
		Snapshots: []fleet.Snapshot{s1, snapshot("r2", t0, 90, fleet.Stale)},
		History:   []fleet.Sample{s1.Last},
		Events:    []fleet.Event{alertEvent(fleet.EventAlertRaised, "r1", fleet.AlertBatteryLow, t0)},
	})
	if err != nil {
		t.Fatal(err)
	}

	snaps, err := st.LoadSnapshots(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snaps) != 2 {
		t.Fatalf("snapshots = %d", len(snaps))
	}
	var got fleet.Snapshot
	for _, s := range snaps {
		if s.RobotID == "r1" {
			got = s
		}
	}
	if got.Last.Battery.Percent != 18.5 || got.Last.Pose != s1.Last.Pose || got.Connectivity != fleet.Online ||
		!got.Last.DeviceTime.Equal(s1.Last.DeviceTime) || len(got.Last.Faults) != 1 || got.Last.Faults[0] != s1.Last.Faults[0] {
		t.Fatalf("round trip mismatch:\n got %+v\nwant %+v", got, s1)
	}
	if len(got.Alerts) != 1 || got.Alerts[0].Key.Kind != fleet.AlertBatteryLow {
		t.Fatalf("alerts = %+v", got.Alerts)
	}

	var historyRows int
	if err := db.pool.QueryRow(ctx, `SELECT count(*) FROM telemetry`).Scan(&historyRows); err != nil || historyRows != 1 {
		t.Fatalf("history rows = %d, %v", historyRows, err)
	}
}

func TestOlderSnapshotDoesNotOverwriteNewer(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	seed(t, db, "r1")
	st := db.State()
	for _, s := range []fleet.Snapshot{snapshot("r1", t0.Add(time.Minute), 50, fleet.Online), snapshot("r1", t0, 10, fleet.Online)} {
		if err := st.WriteBatch(ctx, ingest.Batch{Snapshots: []fleet.Snapshot{s}}); err != nil {
			t.Fatal(err)
		}
	}
	snaps, _ := st.LoadSnapshots(ctx)
	if snaps[0].Last.Battery.Percent != 50 {
		t.Fatalf("battery = %v, older snapshot won", snaps[0].Last.Battery.Percent)
	}
}

func TestAlertWritesAreIdempotent(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	seed(t, db, "r1")
	st := db.State()

	raise := alertEvent(fleet.EventAlertRaised, "r1", fleet.AlertOffline, t0)
	clear := alertEvent(fleet.EventAlertCleared, "r1", fleet.AlertOffline, t0.Add(time.Minute))
	raiseAgain := alertEvent(fleet.EventAlertRaised, "r1", fleet.AlertOffline, t0.Add(2*time.Minute))

	// A batch replayed after an ambiguous failure, then a clear replayed too.
	for _, events := range [][]fleet.Event{{raise}, {raise}, {clear}, {clear}, {raiseAgain}} {
		if err := st.WriteBatch(ctx, ingest.Batch{Events: events}); err != nil {
			t.Fatal(err)
		}
	}

	rows, err := db.pool.Query(ctx, `SELECT raised_at, cleared_at FROM alerts ORDER BY raised_at`)
	if err != nil {
		t.Fatal(err)
	}
	type row struct {
		Raised  time.Time
		Cleared *time.Time
	}
	got, err := pgx.CollectRows(rows, pgx.RowToStructByPos[row])
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Cleared == nil || !got[0].Cleared.Equal(t0.Add(time.Minute)) || got[1].Cleared != nil {
		t.Fatalf("alerts = %+v, want one cleared outage and one active", got)
	}
}
