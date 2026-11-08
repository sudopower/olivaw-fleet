// Package api is the HTTP interface for the presentation layer and for
// provisioning. It owns request and response shapes; the domain types never
// leak onto the wire directly, so they can change without breaking clients.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/sudopower/olivaw-fleet/internal/query"
	"github.com/sudopower/olivaw-fleet/internal/registry"
)

// Deps are what the API needs from the rest of the system.
type Deps struct {
	Registry *registry.Registry
	Query    query.Store
	Ready    func(ctx context.Context) error // e.g. database ping
	Metrics  prometheus.Gatherer
	Log      *slog.Logger

	// HistoryInterval is the finest resolution history is stored at, the
	// smallest telemetry bucket worth asking for.
	HistoryInterval time.Duration
}

type server struct {
	Deps
}

// New returns the HTTP handler.
func New(d Deps) http.Handler {
	s := &server{Deps: d}
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /readyz", s.readyz)
	mux.Handle("GET /metrics", promhttp.HandlerFor(d.Metrics, promhttp.HandlerOpts{}))

	mux.HandleFunc("POST /v1/sites", s.createSite)
	mux.HandleFunc("POST /v1/robots", s.provisionRobot)

	mux.HandleFunc("GET /v1/summary", s.summary)
	mux.HandleFunc("GET /v1/robots", s.listRobots)
	mux.HandleFunc("GET /v1/robots/{id}", s.getRobot)
	mux.HandleFunc("GET /v1/robots/{id}/telemetry", s.robotTelemetry)
	mux.HandleFunc("GET /v1/alerts", s.listAlerts)

	return s.recoverer(mux)
}

func (s *server) readyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.Ready(ctx); err != nil {
		writeError(w, http.StatusServiceUnavailable, "not_ready", err.Error())
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *server) recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil {
				s.Log.Error("panic in handler", "path", r.URL.Path, "panic", v)
				writeError(w, http.StatusInternalServerError, "internal", "internal error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// errorBody is the one error shape every endpoint returns.
type errorBody struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	var b errorBody
	b.Error.Code, b.Error.Message = code, msg
	writeJSON(w, status, b)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// decode reads a JSON body, rejecting unknown fields so client typos fail
// loudly instead of being ignored.
func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return false
	}
	return true
}

// writeDomainError maps registry and storage errors to HTTP.
func (s *server) writeDomainError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, registry.ErrInvalid):
		writeError(w, http.StatusBadRequest, "invalid", err.Error())
	case errors.Is(err, registry.ErrConflict):
		writeError(w, http.StatusConflict, "conflict", err.Error())
	case errors.Is(err, registry.ErrNotFound):
		writeError(w, http.StatusNotFound, "not_found", err.Error())
	default:
		s.Log.Error("request failed", "err", err)
		writeError(w, http.StatusInternalServerError, "internal", "internal error")
	}
}
