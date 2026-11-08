package api

import (
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"time"

	"github.com/sudopower/olivaw-fleet/internal/fleet"
	"github.com/sudopower/olivaw-fleet/internal/query"
)

const (
	defaultLimit = 100
	maxLimit     = 1000
	maxWindow    = 7 * 24 * time.Hour
	chartPoints  = 120 // telemetry buckets per window, enough for a chart
)

var connectivities = []fleet.Connectivity{fleet.Online, fleet.Stale, fleet.Offline, query.NeverSeen}

// badRequest is a validation failure with a message safe to show clients.
type badRequest string

func (e badRequest) Error() string { return string(e) }

func (s *server) fail(w http.ResponseWriter, err error) {
	var br badRequest
	switch {
	case errors.As(err, &br):
		writeError(w, http.StatusBadRequest, "bad_request", br.Error())
	case errors.Is(err, query.ErrNotFound):
		writeError(w, http.StatusNotFound, "not_found", err.Error())
	default:
		s.writeDomainError(w, err)
	}
}

// GET /v1/summary?site_id=
func (s *server) summary(w http.ResponseWriter, r *http.Request) {
	site := fleet.SiteID(r.URL.Query().Get("site_id"))
	sums, err := s.Query.SiteSummaries(r.Context(), site)
	if err != nil {
		s.fail(w, err)
		return
	}
	if site != "" && len(sums) == 0 {
		writeError(w, http.StatusNotFound, "not_found", "site "+string(site)+" not found")
		return
	}
	resp := struct {
		Totals summaryView `json:"totals"`
		Sites  []siteView  `json:"sites"`
	}{Totals: newSummary(), Sites: make([]siteView, 0, len(sums))}
	for _, q := range sums {
		sv := siteView{ID: string(q.SiteID), Name: q.Name, summaryView: newSummary()}
		sv.add(q)
		resp.Totals.add(q)
		resp.Sites = append(resp.Sites, sv)
	}
	writeJSON(w, http.StatusOK, resp)
}

// GET /v1/robots?site_id=&connectivity=&limit=
//
// Robots with active alerts come first. There is no pagination: a site has
// hundreds of robots, not millions, and a fleet manager filters by site.
func (s *server) listRobots(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := query.RobotFilter{SiteID: fleet.SiteID(q.Get("site_id"))}
	var err error
	if f.Limit, err = limit(q.Get("limit")); err != nil {
		s.fail(w, err)
		return
	}
	f.Connectivity = fleet.Connectivity(q.Get("connectivity"))
	if f.Connectivity != "" && !slices.Contains(connectivities, f.Connectivity) {
		s.fail(w, badRequest(fmt.Sprintf("connectivity must be one of %v", connectivities)))
		return
	}

	robots, err := s.Query.ListRobots(r.Context(), f)
	if err != nil {
		s.fail(w, err)
		return
	}
	resp := list[robotView]{Items: make([]robotView, 0, len(robots))}
	for _, rb := range robots {
		resp.Items = append(resp.Items, toRobot(rb))
	}
	writeJSON(w, http.StatusOK, resp)
}

// GET /v1/robots/{id}
func (s *server) getRobot(w http.ResponseWriter, r *http.Request) {
	id := fleet.RobotID(r.PathValue("id"))
	rb, err := s.Query.GetRobot(r.Context(), id)
	if err != nil {
		s.fail(w, err)
		return
	}
	alerts, err := s.Query.ListAlerts(r.Context(), query.AlertFilter{RobotID: id, Limit: 20})
	if err != nil {
		s.fail(w, err)
		return
	}
	resp := struct {
		robotView
		RecentAlerts []alertView `json:"recent_alerts"`
	}{robotView: toRobot(rb), RecentAlerts: make([]alertView, 0, len(alerts))}
	for _, a := range alerts {
		resp.RecentAlerts = append(resp.RecentAlerts, toAlert(a))
	}
	writeJSON(w, http.StatusOK, resp)
}

// GET /v1/robots/{id}/telemetry?window=1h
//
// The last window of history in about 120 buckets, never finer than history
// is stored.
func (s *server) robotTelemetry(w http.ResponseWriter, r *http.Request) {
	id := fleet.RobotID(r.PathValue("id"))
	window := time.Hour
	if v := r.URL.Query().Get("window"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 || d > maxWindow {
			s.fail(w, badRequest(fmt.Sprintf("window must be a duration up to %s", maxWindow)))
			return
		}
		window = d
	}
	to := time.Now().UTC()
	from := to.Add(-window)
	bucket := max(s.HistoryInterval, (window / chartPoints).Round(time.Second))

	if _, err := s.Query.GetRobot(r.Context(), id); err != nil {
		s.fail(w, err)
		return
	}
	points, err := s.Query.Telemetry(r.Context(), id, from, to, bucket)
	if err != nil {
		s.fail(w, err)
		return
	}
	resp := struct {
		From   time.Time   `json:"from"`
		To     time.Time   `json:"to"`
		Bucket string      `json:"bucket"`
		Points []pointView `json:"points"`
	}{From: from, To: to, Bucket: bucket.String(), Points: make([]pointView, 0, len(points))}
	for _, p := range points {
		resp.Points = append(resp.Points, pointView{
			At: p.At.UTC(), BatteryAvg: round1(p.BatteryAvg), BatteryMin: round1(p.BatteryMin), Charging: p.Charging,
			Mode: string(p.Mode), Samples: p.Samples,
		})
	}
	writeJSON(w, http.StatusOK, resp)
}

// GET /v1/alerts?site_id=&robot_id=&state=active|all&limit=
func (s *server) listAlerts(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := query.AlertFilter{SiteID: fleet.SiteID(q.Get("site_id")), RobotID: fleet.RobotID(q.Get("robot_id"))}
	var err error
	if f.Limit, err = limit(q.Get("limit")); err != nil {
		s.fail(w, err)
		return
	}
	switch q.Get("state") {
	case "", "active":
		f.ActiveOnly = true
	case "all":
	default:
		s.fail(w, badRequest("state must be active or all"))
		return
	}
	alerts, err := s.Query.ListAlerts(r.Context(), f)
	if err != nil {
		s.fail(w, err)
		return
	}
	resp := list[alertView]{Items: make([]alertView, 0, len(alerts))}
	for _, a := range alerts {
		resp.Items = append(resp.Items, toAlert(a))
	}
	writeJSON(w, http.StatusOK, resp)
}

func limit(v string) (int, error) {
	if v == "" {
		return defaultLimit, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 || n > maxLimit {
		return 0, badRequest(fmt.Sprintf("limit must be between 1 and %d", maxLimit))
	}
	return n, nil
}
