package ingest

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/sudopower/olivaw-fleet/internal/fleet"
)

const (
	resultAccepted     = "accepted"
	resultDuplicate    = "duplicate"
	resultOutOfOrder   = "out_of_order"
	resultInvalid      = "invalid"
	resultMalformed    = "malformed"
	resultUnrouted     = "unrouted"
	resultUnknownRobot = "unknown_robot"
	resultDropped      = "dropped_backpressure"
	resultDisconnect   = "disconnect"
)

// Metrics are the pipeline's operational signals. Every message ends up in
// exactly one Messages result, so the counters add up to what came in.
type Metrics struct {
	Messages     *prometheus.CounterVec
	FlushSeconds prometheus.Histogram
	FlushErrors  prometheus.Counter

	results map[string]prometheus.Counter
}

// NewMetrics registers the pipeline metrics with reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		Messages: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "fleet_ingest_messages_total",
			Help: "Messages received, by what happened to them.",
		}, []string{"result"}),
		FlushSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "fleet_ingest_flush_seconds",
			Help:    "Time to write one batch.",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 14),
		}),
		FlushErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fleet_ingest_flush_errors_total",
			Help: "Failed batch writes. State and events are retried.",
		}),
	}
	reg.MustRegister(m.Messages, m.FlushSeconds, m.FlushErrors)

	// Resolve label lookups once; result() is on the hot path.
	m.results = make(map[string]prometheus.Counter)
	for _, r := range []string{resultAccepted, resultDuplicate, resultOutOfOrder, resultInvalid, resultMalformed,
		resultUnrouted, resultUnknownRobot, resultDropped, resultDisconnect} {
		m.results[r] = m.Messages.WithLabelValues(r)
	}
	return m
}

func (m *Metrics) result(r string) { m.results[r].Inc() }

func (m *Metrics) outcome(o fleet.Outcome) {
	switch o {
	case fleet.Accepted:
		m.result(resultAccepted)
	case fleet.Duplicate:
		m.result(resultDuplicate)
	case fleet.OutOfOrder:
		m.result(resultOutOfOrder)
	}
}
