package telemetry

import (
	"errors"
	"testing"
	"time"

	"github.com/sudopower/olivaw-fleet/internal/fleet"
)

var recv = time.Date(2026, 11, 8, 12, 0, 1, 0, time.UTC)

func router() *Router { return NewRouter(VDA5050{}) }

func TestRoute(t *testing.T) {
	cases := []struct {
		topic string
		ok    bool
		dev   Device
		kind  Kind
	}{
		{"uagv/v2/acme/SN-1/state", true, Device{"acme", "SN-1"}, KindState},
		{"uagv/v2/acme/SN-1/connection", true, Device{"acme", "SN-1"}, KindConnection},
		{"uagv/v2/acme/SN-1/order", false, Device{}, 0}, // not ours: we never send orders
		{"uagv/v1/acme/SN-1/state", false, Device{}, 0},
		{"uagv/v2//SN-1/state", false, Device{}, 0},
		{"something/else", false, Device{}, 0},
	}
	for _, c := range cases {
		env, ok := router().Route(c.topic, nil, recv)
		if ok != c.ok || env.Device != c.dev || env.Kind != c.kind {
			t.Errorf("Route(%q) = %+v %v, want %+v %v %v", c.topic, env.Device, ok, c.dev, c.kind, c.ok)
		}
	}
}

func decode(t *testing.T, topic, payload string) Decoded {
	t.Helper()
	env, ok := router().Route(topic, []byte(payload), recv)
	if !ok {
		t.Fatalf("no route for %q", topic)
	}
	d, err := env.Decode()
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	return d
}

func TestVDA5050State(t *testing.T) {
	d := decode(t, "uagv/v2/acme/SN-1/state", `{
		"headerId": 42, "timestamp": "2026-11-08T12:00:00.250Z", "version": "2.0.0",
		"manufacturer": "acme", "serialNumber": "SN-1",
		"operatingMode": "AUTOMATIC", "driving": true,
		"agvPosition": {"x": 12.5, "y": -3, "theta": 1.57, "mapId": "floor-2", "positionInitialized": true},
		"batteryState": {"batteryCharge": 64.5, "charging": false},
		"errors": [{"errorType": "lidarDirty", "errorLevel": "WARNING", "errorDescription": "clean the lidar"}],
		"loads": [], "actionStates": []
	}`)
	s := d.Sample
	if want := time.Date(2026, 11, 8, 12, 0, 0, 250e6, time.UTC); !s.DeviceTime.Equal(want) {
		t.Errorf("device time = %v", s.DeviceTime)
	}
	if !s.ReceivedAt.Equal(recv) {
		t.Errorf("received = %v", s.ReceivedAt)
	}
	if s.Battery != (fleet.Battery{Percent: 64.5}) {
		t.Errorf("battery = %+v", s.Battery)
	}
	if s.Pose != (fleet.Pose{MapID: "floor-2", X: 12.5, Y: -3, Heading: 1.57}) {
		t.Errorf("pose = %+v", s.Pose)
	}
	if s.Mode != fleet.ModeWorking {
		t.Errorf("mode = %v", s.Mode)
	}
	if len(s.Faults) != 1 || s.Faults[0] != (fleet.Fault{Code: "lidarDirty", Severity: fleet.SeverityWarning, Message: "clean the lidar"}) {
		t.Errorf("faults = %+v", s.Faults)
	}
	if s.RobotID != "" {
		t.Errorf("adapter must not assign robot identity, got %q", s.RobotID)
	}
}

func TestVDA5050Mode(t *testing.T) {
	cases := []struct {
		name, body string
		want       fleet.Mode
	}{
		{"fatal error wins", `"operatingMode":"AUTOMATIC","driving":true,"batteryState":{"charging":true},"errors":[{"errorType":"x","errorLevel":"FATAL"}]`, fleet.ModeError},
		{"manual", `"operatingMode":"MANUAL","driving":true`, fleet.ModeManual},
		{"charging", `"operatingMode":"AUTOMATIC","batteryState":{"charging":true}`, fleet.ModeCharging},
		{"idle", `"operatingMode":"AUTOMATIC","driving":false`, fleet.ModeIdle},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d := decode(t, "uagv/v2/acme/SN-1/state", `{"timestamp":"2026-11-08T12:00:00Z",`+c.body+`}`)
			if d.Sample.Mode != c.want {
				t.Errorf("mode = %v, want %v", d.Sample.Mode, c.want)
			}
		})
	}
}

func TestVDA5050UninitializedPositionIsIgnored(t *testing.T) {
	d := decode(t, "uagv/v2/acme/SN-1/state",
		`{"timestamp":"2026-11-08T12:00:00Z","agvPosition":{"x":99,"y":99,"positionInitialized":false}}`)
	if d.Sample.Pose != (fleet.Pose{}) {
		t.Errorf("pose = %+v, want zero: robot does not know where it is", d.Sample.Pose)
	}
}

func TestVDA5050Connection(t *testing.T) {
	for state, want := range map[string]bool{"CONNECTIONBROKEN": true, "OFFLINE": true, "ONLINE": false} {
		d := decode(t, "uagv/v2/acme/SN-1/connection", `{"timestamp":"2026-11-08T12:00:00Z","connectionState":"`+state+`"}`)
		if d.Disconnected != want {
			t.Errorf("%s: disconnected = %v", state, d.Disconnected)
		}
	}
}

func TestMalformed(t *testing.T) {
	cases := map[string]string{
		"uagv/v2/acme/SN-1/state":      `{"timestamp": "not a time"}`,
		"uagv/v2/acme/SN-1/connection": `{"connectionState": "MAYBE"}`,
	}
	for topic, payload := range cases {
		env, _ := router().Route(topic, []byte(payload), recv)
		if _, err := env.Decode(); !errors.Is(err, ErrMalformed) {
			t.Errorf("%s: err = %v", topic, err)
		}
	}
}
