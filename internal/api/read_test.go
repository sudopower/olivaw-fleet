package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/sudopower/olivaw-fleet/internal/fleet"
	"github.com/sudopower/olivaw-fleet/internal/query"
)

// fakeQuery records the last robot filter it got.
type fakeQuery struct {
	query.Store
	lastFilter query.RobotFilter
}

func (f *fakeQuery) ListRobots(_ context.Context, rf query.RobotFilter) ([]query.Robot, error) {
	f.lastFilter = rf
	return []query.Robot{{ID: "r1", Connectivity: query.NeverSeen}}, nil
}

func testServer(q query.Store) http.Handler {
	return New(Deps{
		Query: q, Metrics: prometheus.NewRegistry(),
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)), HistoryInterval: 5 * time.Second,
		Ready: func(context.Context) error { return nil },
	})
}

func get(t *testing.T, h http.Handler, url string, into any) int {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, url, nil))
	if into != nil {
		if err := json.Unmarshal(rec.Body.Bytes(), into); err != nil {
			t.Fatalf("%s: %v: %s", url, err, rec.Body)
		}
	}
	return rec.Code
}

func TestValidation(t *testing.T) {
	q := &fakeQuery{}
	h := testServer(q)
	for _, url := range []string{
		"/v1/robots?limit=0",
		"/v1/robots?limit=1001",
		"/v1/robots?connectivity=sleeping",
		"/v1/alerts?state=some",
		"/v1/robots/r1/telemetry?window=soon",
		"/v1/robots/r1/telemetry?window=720h",
	} {
		var body errorBody
		if code := get(t, h, url, &body); code != http.StatusBadRequest || body.Error.Code != "bad_request" {
			t.Errorf("%s: status %d, body %+v", url, code, body)
		}
	}

	var resp list[robotView]
	if code := get(t, h, "/v1/robots?site_id=s1&connectivity=offline", &resp); code != http.StatusOK || len(resp.Items) != 1 {
		t.Fatalf("status %d, body %+v", code, resp)
	}
	if f := q.lastFilter; f.SiteID != "s1" || f.Connectivity != fleet.Offline || f.Limit != defaultLimit {
		t.Fatalf("filter = %+v", f)
	}
}
