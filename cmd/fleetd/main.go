// Command fleetd ingests robot telemetry and serves fleet state.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/sudopower/olivaw-fleet/internal/api"
	"github.com/sudopower/olivaw-fleet/internal/ingest"
	"github.com/sudopower/olivaw-fleet/internal/postgres"
	"github.com/sudopower/olivaw-fleet/internal/registry"
	"github.com/sudopower/olivaw-fleet/internal/telemetry"
	"github.com/sudopower/olivaw-fleet/internal/transport/mqtt"
)

type config struct {
	httpAddr string
	dbURL    string
	mqttURL  string
	ingest   ingest.Config
}

func parseFlags() config {
	c := config{ingest: ingest.DefaultConfig()}
	flag.StringVar(&c.httpAddr, "http", env("FLEET_HTTP_ADDR", ":8080"), "HTTP listen address")
	flag.StringVar(&c.dbURL, "db", env("FLEET_DATABASE_URL", "postgres://fleet:fleet@localhost:5433/fleet?sslmode=disable"), "Postgres URL")
	flag.StringVar(&c.mqttURL, "mqtt", env("FLEET_MQTT_URL", "tcp://localhost:1883"), "MQTT broker URL")
	flag.DurationVar(&c.ingest.FlushInterval, "flush-interval", c.ingest.FlushInterval, "state flush interval")
	flag.DurationVar(&c.ingest.HistoryInterval, "history-interval", c.ingest.HistoryInterval, "history resolution per robot")
	flag.Parse()
	return c
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	if err := run(parseFlags(), log); err != nil {
		log.Error("fleetd stopped", "err", err)
		os.Exit(1)
	}
}

func run(cfg config, log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	startedAt := time.Now()

	db, err := postgres.Open(ctx, cfg.dbURL)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := db.Migrate(ctx); err != nil {
		return err
	}

	reg := registry.New(db.Registry())
	if err := reg.Load(ctx); err != nil {
		return err
	}
	log.Info("registry loaded", "robots", reg.Len())

	promReg := prometheus.NewRegistry()
	promReg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	router := telemetry.NewRouter(telemetry.VDA5050{})
	pipe := ingest.New(cfg.ingest, router, reg, db.State(), ingest.NewMetrics(promReg), log)
	if err := pipe.Restore(ctx, startedAt); err != nil {
		return err
	}

	// The pipeline gets its own context so it can finish its final flush
	// after MQTT has disconnected.
	pipeCtx, stopPipe := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Go(func() { pipe.Run(pipeCtx) })

	host, _ := os.Hostname()
	sub, err := mqtt.Subscribe(ctx, cfg.mqttURL, fmt.Sprintf("fleetd-%s-%d", host, os.Getpid()),
		router.Subscriptions(), pipe.Handle, log)
	if err != nil {
		stopPipe()
		wg.Wait()
		return err
	}

	srv := &http.Server{
		Addr:              cfg.httpAddr,
		Handler:           api.New(api.Deps{Registry: reg, Ready: db.Ping, Metrics: promReg, Log: log}),
		ReadHeaderTimeout: 5 * time.Second,
	}
	httpErr := make(chan error, 1)
	go func() { httpErr <- srv.ListenAndServe() }()
	log.Info("fleetd running", "http", cfg.httpAddr, "mqtt", cfg.mqttURL)

	select {
	case <-ctx.Done():
	case err = <-httpErr:
	}

	// Shut down in dependency order: stop taking messages, flush what was
	// taken, then stop serving reads.
	log.Info("shutting down")
	sub.Close()
	stopPipe()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	stop()
	wg.Wait()
	if errors.Is(err, http.ErrServerClosed) {
		err = nil
	}
	return err
}

func env(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}
