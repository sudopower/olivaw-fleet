// Command simulator fakes a robot fleet: just enough robot to exercise
// fleetd. It registers sites and robots through the API, then publishes a
// VDA 5050 state message per robot per interval over MQTT, with the
// misbehaviour real fleets show: duplicates, reordering, silence, broken
// connections, wrong clocks and a robot nobody provisioned.
//
//	go run ./cmd/simulator -robots 300 -sites 4
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"

	"github.com/sudopower/olivaw-fleet/internal/telemetry"
)

const (
	drainPerSec  = 0.25 // %/s, exaggerated so a battery cycle takes minutes
	chargePerSec = 0.8
	dockAt       = 22.0 // a robot goes to charge below this
	lemonDockAt  = 7.0  // 5% are misconfigured and run low enough to alert
	resumeAt     = 95.0
)

// chaos holds per-message probabilities, except fault, which is per second.
type chaos struct {
	duplicate, reorder, dropout, clockJump, fault float64
}

var defaultChaos = chaos{duplicate: 0.01, reorder: 0.005, dropout: 1.0 / 4000, clockJump: 1.0 / 20000, fault: 1.0 / 3000}

type robot struct {
	maker, serial, site string

	clock       time.Duration // how wrong the robot's clock is
	header      uint64
	x, y        float64
	battery     float64
	dockAt      float64
	charging    bool
	faultUntil  time.Time // a fatal fault is active until then
	silentUntil time.Time
	held        []byte // state held back, sent after the next one
}

type stats struct{ published, duplicated, reordered, dropouts, clockJumps int }

type sim struct {
	robots []*robot
	chaos  chaos
	rng    *rand.Rand
	mqtt   paho.Client
	stats  stats
}

func main() {
	var (
		mqttURL  = flag.String("mqtt", "tcp://localhost:1883", "MQTT broker URL")
		apiURL   = flag.String("api", "http://localhost:8080", "fleetd API URL, for provisioning")
		robots   = flag.Int("robots", 200, "number of robots")
		sites    = flag.Int("sites", 3, "number of sites")
		interval = flag.Duration("interval", time.Second, "report interval per robot")
		noChaos  = flag.Bool("no-chaos", false, "no duplicates, reordering, dropouts, clock errors or rogue robot")
	)
	flag.Parse()
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	s := &sim{rng: rand.New(rand.NewPCG(1, 1)), chaos: defaultChaos}
	if *noChaos {
		s.chaos = chaos{fault: defaultChaos.fault} // robots still fault
	}
	for i := range *robots {
		s.robots = append(s.robots, s.newRobot(fmt.Sprintf("SN-%05d", i), fmt.Sprintf("site-%02d", i%*sites+1)))
	}
	if err := provision(ctx, *apiURL, *sites, s.robots); err != nil {
		log.Error("provisioning failed", "err", err)
		os.Exit(1)
	}
	log.Info("provisioned", "robots", len(s.robots))
	if !*noChaos {
		// Publishes like the others but was never provisioned.
		s.robots = append(s.robots, s.newRobot("SN-ROGUE", "site-01"))
	}

	s.mqtt = paho.NewClient(paho.NewClientOptions().AddBroker(*mqttURL).
		SetClientID(fmt.Sprintf("simulator-%d", os.Getpid())).SetAutoReconnect(true))
	if tok := s.mqtt.Connect(); tok.Wait() && tok.Error() != nil {
		log.Error("mqtt", "err", tok.Error())
		os.Exit(1)
	}
	defer s.mqtt.Disconnect(250)

	// Each tick publishes a tenth of the fleet, to spread messages over the
	// interval instead of sending one burst.
	const groups = 10
	tick := time.NewTicker(*interval / groups)
	defer tick.Stop()
	report := time.NewTicker(10 * time.Second)
	defer report.Stop()
	log.Info("publishing", "robots", len(s.robots), "interval", *interval)
	group := 0
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-tick.C:
			for i := group; i < len(s.robots); i += groups {
				s.tick(s.robots[i], now, *interval)
			}
			group = (group + 1) % groups
		case <-report.C:
			log.Info("stats", "published", s.stats.published, "duplicated", s.stats.duplicated,
				"reordered", s.stats.reordered, "dropouts", s.stats.dropouts, "clock_jumps", s.stats.clockJumps)
		}
	}
}

func (s *sim) newRobot(serial, site string) *robot {
	makers := []string{"acme", "nordbot"}
	r := &robot{
		maker: makers[s.rng.IntN(2)], serial: serial, site: site,
		x: s.rng.Float64() * 50, y: s.rng.Float64() * 30,
		battery: 30 + s.rng.Float64()*70, dockAt: dockAt,
	}
	if s.rng.Float64() < 0.05 {
		r.dockAt = lemonDockAt
	}
	if s.rng.Float64() < 0.1 {
		r.clock = time.Duration(s.rng.IntN(240)-120) * time.Second
	}
	return r
}

// step advances the robot: work and drain, charge at the dock, or stand
// still while a fatal fault is active.
func (r *robot) step(now time.Time, dt time.Duration, c chaos, rng *rand.Rand) {
	sec := dt.Seconds()
	switch {
	case r.charging:
		r.battery = min(100, r.battery+chargePerSec*sec)
		r.charging = r.battery < resumeAt
	case now.Before(r.faultUntil):
	default:
		r.battery = max(0, r.battery-drainPerSec*sec*(0.8+0.4*rng.Float64()))
		r.x = min(50, max(0, r.x+rng.NormFloat64()))
		r.y = min(30, max(0, r.y+rng.NormFloat64()))
		if r.battery < r.dockAt {
			r.charging, r.x, r.y = true, 0, 0
		}
	}
	if !now.Before(r.faultUntil) && rng.Float64() < c.fault*sec {
		r.faultUntil = now.Add(time.Duration(20+rng.IntN(100)) * time.Second)
	}
}

func (s *sim) tick(r *robot, now time.Time, dt time.Duration) {
	r.step(now, dt, s.chaos, s.rng)
	if now.Before(r.silentUntil) {
		return
	}
	if s.chance(s.chaos.dropout) {
		s.stats.dropouts++
		r.silentUntil = now.Add(15*time.Second + time.Duration(s.rng.Int64N(int64(75*time.Second))))
		// Half the time the connection breaks visibly and the broker would
		// send the last-will; otherwise the robot just stops talking.
		if s.chance(0.5) {
			s.publish(r.topic("connection"), 1, r.connectionBroken(now))
		}
		return
	}
	if s.chance(s.chaos.clockJump) {
		s.stats.clockJumps++
		r.clock -= time.Minute // an NTP correction
	}

	msg := r.state(now)
	switch {
	case r.held == nil && s.chance(s.chaos.reorder):
		s.stats.reordered++
		r.held = msg
		return
	case s.chance(s.chaos.duplicate):
		s.stats.duplicated++
		s.publish(r.topic("state"), 0, msg)
	}
	s.stats.published++
	s.publish(r.topic("state"), 0, msg)
	if r.held != nil {
		s.publish(r.topic("state"), 0, r.held)
		r.held = nil
	}
}

func (s *sim) chance(p float64) bool { return p > 0 && s.rng.Float64() < p }

func (s *sim) publish(topic string, qos byte, payload []byte) {
	s.mqtt.Publish(topic, qos, false, payload)
}

func (r *robot) topic(kind string) string {
	return fmt.Sprintf("uagv/v2/%s/%s/%s", r.maker, r.serial, kind)
}

func (r *robot) state(now time.Time) []byte {
	r.header++
	m := telemetry.VDA5050State{
		HeaderID:      r.header,
		Timestamp:     now.Add(r.clock).UTC().Truncate(time.Millisecond),
		Version:       "2.0.0",
		Manufacturer:  r.maker,
		SerialNumber:  r.serial,
		OperatingMode: "AUTOMATIC",
		Driving:       !r.charging && !now.Before(r.faultUntil),
		AGVPosition: &telemetry.VDA5050Position{
			X: float64(int(r.x*100)) / 100, Y: float64(int(r.y*100)) / 100,
			MapID: r.site + "-main", PositionInitialized: true,
		},
		BatteryState: telemetry.VDA5050Battery{BatteryCharge: float64(int(r.battery*10)) / 10, Charging: r.charging},
		Errors:       []telemetry.VDA5050Error{},
	}
	if now.Before(r.faultUntil) {
		m.Errors = append(m.Errors, telemetry.VDA5050Error{
			ErrorType: "localizationLost", ErrorLevel: "FATAL", ErrorDescription: "localization lost",
		})
	}
	b, _ := json.Marshal(m)
	return b
}

// connectionBroken is the message the broker publishes as the robot's MQTT
// last-will. The simulator shares one connection between all robots, so it
// sends it itself.
func (r *robot) connectionBroken(now time.Time) []byte {
	r.header++
	b, _ := json.Marshal(telemetry.VDA5050Connection{
		HeaderID: r.header, Timestamp: now.Add(r.clock).UTC(), Version: "2.0.0",
		Manufacturer: r.maker, SerialNumber: r.serial, ConnectionState: "CONNECTIONBROKEN",
	})
	return b
}

// provision registers sites and robots. 409 means they already exist, so
// running the simulator again is safe.
func provision(ctx context.Context, api string, sites int, robots []*robot) error {
	for i := range sites {
		id := fmt.Sprintf("site-%02d", i+1)
		if err := post(ctx, api+"/v1/sites", map[string]string{"id": id, "name": "Site " + id[5:]}); err != nil {
			return err
		}
	}
	for _, r := range robots {
		body := map[string]string{"site_id": r.site, "manufacturer": r.maker, "serial": r.serial}
		if err := post(ctx, api+"/v1/robots", body); err != nil {
			return err
		}
	}
	return nil
}

func post(ctx context.Context, url string, body any) error {
	b, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body) // lets the connection be reused
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusConflict {
		return fmt.Errorf("POST %s: %s", url, resp.Status)
	}
	return nil
}
