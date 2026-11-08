// Package ingest moves telemetry from the transport into derived state and
// storage.
//
//	MQTT ──► Handle ──► inbox ──► Run loop ──────────────► Store
//	         (route,              (decode, validate,       (one transaction
//	          resolve)             domain rules)            per flush)
//
// One goroutine owns every fleet.Robot, so there is no locking and samples
// for a robot are handled in arrival order. That is enough for tens of
// thousands of robots on one core; ADR 0005 describes how this splits by
// hash(robot) when it is not.
//
// The pipeline treats three kinds of data differently (ADR 0003):
//
//   - Current state is coalesced: a robot that reported ten times since the
//     last flush is written once, with its latest state.
//   - History is decimated: one sample per robot per HistoryInterval, plus any
//     sample that changed mode or caused an event.
//   - Events are never dropped or coalesced. If a write fails they are kept
//     and retried with the next flush.
package ingest

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/sudopower/olivaw-fleet/internal/fleet"
	"github.com/sudopower/olivaw-fleet/internal/registry"
	"github.com/sudopower/olivaw-fleet/internal/telemetry"
)

// Config tunes the pipeline.
type Config struct {
	Buffer          int           // inbox capacity before samples are dropped
	FlushInterval   time.Duration // how often state is written
	TickInterval    time.Duration // how often silence is re-judged
	HistoryInterval time.Duration // history resolution per robot
	Policy          fleet.Policy
}

// DefaultConfig suits a laptop and a few thousand robots.
func DefaultConfig() Config {
	return Config{
		Buffer:          16_384,
		FlushInterval:   time.Second,
		TickInterval:    time.Second,
		HistoryInterval: 5 * time.Second,
		Policy:          fleet.DefaultPolicy(),
	}
}

// Batch is one flush worth of writes. Store must apply it atomically.
type Batch struct {
	Snapshots []fleet.Snapshot // latest state of every robot that changed
	History   []fleet.Sample   // decimated samples, RobotID set
	Events    []fleet.Event    // in the order they happened per robot
}

// Store persists flushed batches and restores state at startup.
type Store interface {
	WriteBatch(ctx context.Context, b Batch) error
	LoadSnapshots(ctx context.Context) ([]fleet.Snapshot, error)
}

// Resolver maps wire identities to provisioned robots.
type Resolver interface {
	Resolve(d telemetry.Device) (registry.Robot, bool)
}

type inbound struct {
	env     telemetry.Envelope
	robotID fleet.RobotID
	siteID  fleet.SiteID
}

type tracked struct {
	robot       *fleet.Robot
	lastHistory time.Time // server time of the last sample kept for history
	lastMode    fleet.Mode
}

// Pipeline is the ingest engine. Create with New, call Restore, then Run.
type Pipeline struct {
	cfg      Config
	router   *telemetry.Router
	resolver Resolver
	store    Store
	metrics  *Metrics
	log      *slog.Logger

	in   chan inbound
	stop chan struct{} // closed when Run returns; unblocks senders

	// Owned by the Run goroutine.
	robots  map[fleet.RobotID]*tracked
	dirty   map[fleet.RobotID]struct{}
	history []fleet.Sample
	events  []fleet.Event
}

func New(cfg Config, router *telemetry.Router, resolver Resolver, store Store, m *Metrics, log *slog.Logger) *Pipeline {
	return &Pipeline{
		cfg: cfg, router: router, resolver: resolver, store: store, metrics: m, log: log,
		in:     make(chan inbound, cfg.Buffer),
		stop:   make(chan struct{}),
		robots: make(map[fleet.RobotID]*tracked),
		dirty:  make(map[fleet.RobotID]struct{}),
	}
}

// Restore loads persisted state. Call before Run. startedAt protects robots
// from being timed out for the service's own downtime (see fleet.Restore).
func (p *Pipeline) Restore(ctx context.Context, startedAt time.Time) error {
	snaps, err := p.store.LoadSnapshots(ctx)
	if err != nil {
		return err
	}
	for _, s := range snaps {
		p.robots[s.RobotID] = &tracked{
			robot:       fleet.Restore(s, startedAt),
			lastHistory: s.LastSeen,
			lastMode:    s.Last.Mode,
		}
	}
	p.log.Info("restored robot state", "robots", len(snaps))
	return nil
}

// Handle is the transport callback. It must stay cheap: it parses the topic,
// resolves the robot and queues the raw payload.
func (p *Pipeline) Handle(topic string, payload []byte, receivedAt time.Time) {
	env, ok := p.router.Route(topic, payload, receivedAt)
	if !ok {
		p.metrics.result(resultUnrouted)
		return
	}
	rb, ok := p.resolver.Resolve(env.Device)
	if !ok {
		p.metrics.result(resultUnknownRobot)
		return
	}
	in := inbound{env: env, robotID: rb.ID, siteID: rb.SiteID}

	if env.Kind == telemetry.KindState {
		// Samples carry full state, so dropping one under overload loses
		// nothing the next one does not restore. Blocking instead would push
		// the backlog into the MQTT client and stall every robot.
		select {
		case p.in <- in:
		default:
			p.metrics.result(resultDropped)
		}
		return
	}
	// Connection messages are rare and not repeated: wait for room.
	select {
	case p.in <- in:
	case <-p.stop:
	}
}

// Run processes messages until ctx is cancelled, then flushes once more and
// returns. A flush runs on this goroutine too; while it writes, messages wait
// in the inbox.
func (p *Pipeline) Run(ctx context.Context) {
	defer close(p.stop)
	tick := time.NewTicker(p.cfg.TickInterval)
	defer tick.Stop()
	flush := time.NewTicker(p.cfg.FlushInterval)
	defer flush.Stop()
	for {
		select {
		case m := <-p.in:
			p.handle(m)
		case now := <-tick.C:
			p.tick(now)
		case <-flush.C:
			p.flush(ctx)
		case <-ctx.Done():
			final, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			p.flush(final)
			if len(p.events) > 0 {
				p.log.Error("shutdown with unwritten events", "events", len(p.events))
			}
			return
		}
	}
}

func (p *Pipeline) get(id fleet.RobotID, site fleet.SiteID) *tracked {
	t, ok := p.robots[id]
	if !ok {
		t = &tracked{robot: fleet.NewRobot(id, site)}
		p.robots[id] = t
	}
	return t
}

func (p *Pipeline) handle(m inbound) {
	dec, err := m.env.Decode()
	if err != nil {
		p.metrics.result(resultMalformed)
		return
	}
	t := p.get(m.robotID, m.siteID)

	if m.env.Kind == telemetry.KindConnection {
		if dec.Disconnected {
			p.metrics.result(resultDisconnect)
			p.record(m.robotID, t.robot.Disconnect(m.env.ReceivedAt))
		}
		return
	}

	sample := dec.Sample
	sample.RobotID = m.robotID
	if err := sample.Validate(); err != nil {
		p.metrics.result(resultInvalid)
		return
	}

	outcome, events := t.robot.Observe(sample, p.cfg.Policy)
	p.metrics.outcome(outcome)
	if outcome != fleet.Accepted {
		return
	}
	p.dirty[m.robotID] = struct{}{}
	p.events = append(p.events, events...)

	if len(events) > 0 || sample.Mode != t.lastMode ||
		sample.ReceivedAt.Sub(t.lastHistory) >= p.cfg.HistoryInterval {
		p.history = append(p.history, sample)
		t.lastHistory = sample.ReceivedAt
		t.lastMode = sample.Mode
	}
}

// tick re-judges every robot's connectivity: a cheap comparison per robot
// per second.
func (p *Pipeline) tick(now time.Time) {
	for id, t := range p.robots {
		p.record(id, t.robot.Tick(now, p.cfg.Policy))
	}
}

func (p *Pipeline) record(id fleet.RobotID, events []fleet.Event) {
	if len(events) == 0 {
		return
	}
	p.dirty[id] = struct{}{}
	p.events = append(p.events, events...)
}

// flush writes everything since the last successful flush in one
// transaction. On failure, dirty robots and events are kept for the next
// attempt (state is re-read then, so it is still coalesced). History is
// dropped instead: it is decimated trend data, and holding it would grow
// memory for as long as the database is down.
func (p *Pipeline) flush(ctx context.Context) {
	if len(p.dirty) == 0 && len(p.history) == 0 && len(p.events) == 0 {
		return
	}
	b := Batch{History: p.history, Events: p.events, Snapshots: make([]fleet.Snapshot, 0, len(p.dirty))}
	for id := range p.dirty {
		b.Snapshots = append(b.Snapshots, p.robots[id].robot.Snapshot())
	}

	start := time.Now()
	err := p.store.WriteBatch(ctx, b)
	p.metrics.FlushSeconds.Observe(time.Since(start).Seconds())
	p.history = nil
	if err != nil {
		p.metrics.FlushErrors.Inc()
		if !errors.Is(err, context.Canceled) {
			p.log.Error("flush failed, will retry", "err", err,
				"snapshots", len(b.Snapshots), "history_dropped", len(b.History), "events", len(b.Events))
		}
		return
	}
	clear(p.dirty)
	p.events = nil
}
