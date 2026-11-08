package telemetry

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/sudopower/olivaw-fleet/internal/fleet"
)

// VDA5050 handles robots speaking (a subset of) VDA 5050 v2, the German
// interface standard for mobile robots. Topics follow the standard layout:
//
//	uagv/v2/{manufacturer}/{serialNumber}/state
//	uagv/v2/{manufacturer}/{serialNumber}/connection
//
// The connection topic is where robots register an MQTT last-will message of
// CONNECTIONBROKEN, so the broker announces a dead robot within its keepalive
// instead of the platform waiting out the silence.
type VDA5050 struct{}

// VDA5050State is the subset of the VDA 5050 state message this platform uses.
type VDA5050State struct {
	HeaderID      uint64           `json:"headerId"`
	Timestamp     time.Time        `json:"timestamp"`
	Version       string           `json:"version"`
	Manufacturer  string           `json:"manufacturer"`
	SerialNumber  string           `json:"serialNumber"`
	OperatingMode string           `json:"operatingMode"` // AUTOMATIC, SEMIAUTOMATIC, MANUAL, SERVICE, TEACHIN
	Driving       bool             `json:"driving"`
	AGVPosition   *VDA5050Position `json:"agvPosition,omitempty"`
	BatteryState  VDA5050Battery   `json:"batteryState"`
	Errors        []VDA5050Error   `json:"errors"`
}

type VDA5050Position struct {
	X                   float64 `json:"x"`
	Y                   float64 `json:"y"`
	Theta               float64 `json:"theta"`
	MapID               string  `json:"mapId"`
	PositionInitialized bool    `json:"positionInitialized"`
}

type VDA5050Battery struct {
	BatteryCharge float64 `json:"batteryCharge"` // percent
	Charging      bool    `json:"charging"`
}

type VDA5050Error struct {
	ErrorType        string `json:"errorType"`
	ErrorLevel       string `json:"errorLevel"` // WARNING or FATAL
	ErrorDescription string `json:"errorDescription,omitempty"`
}

// VDA5050Connection is the connection-topic message.
type VDA5050Connection struct {
	HeaderID        uint64    `json:"headerId"`
	Timestamp       time.Time `json:"timestamp"`
	Version         string    `json:"version"`
	Manufacturer    string    `json:"manufacturer"`
	SerialNumber    string    `json:"serialNumber"`
	ConnectionState string    `json:"connectionState"` // ONLINE, OFFLINE, CONNECTIONBROKEN
}

func (VDA5050) Subscriptions() []string {
	return []string{"uagv/v2/+/+/state", "uagv/v2/+/+/connection"}
}

func (VDA5050) Route(topic string) (Device, Kind, bool) {
	parts := strings.Split(topic, "/")
	if len(parts) != 5 || parts[0] != "uagv" || parts[1] != "v2" || parts[2] == "" || parts[3] == "" {
		return Device{}, 0, false
	}
	dev := Device{Manufacturer: parts[2], Serial: parts[3]}
	switch parts[4] {
	case "state":
		return dev, KindState, true
	case "connection":
		return dev, KindConnection, true
	}
	return Device{}, 0, false
}

func (VDA5050) Decode(kind Kind, payload []byte, receivedAt time.Time) (Decoded, error) {
	switch kind {
	case KindState:
		var m VDA5050State
		if err := json.Unmarshal(payload, &m); err != nil {
			return Decoded{}, fmt.Errorf("%w: vda5050 state: %v", ErrMalformed, err)
		}
		return Decoded{Sample: m.toSample(receivedAt)}, nil
	case KindConnection:
		var m VDA5050Connection
		if err := json.Unmarshal(payload, &m); err != nil {
			return Decoded{}, fmt.Errorf("%w: vda5050 connection: %v", ErrMalformed, err)
		}
		switch m.ConnectionState {
		case "OFFLINE", "CONNECTIONBROKEN":
			return Decoded{Disconnected: true}, nil
		case "ONLINE":
			return Decoded{}, nil
		}
		return Decoded{}, fmt.Errorf("%w: vda5050 connection state %q", ErrMalformed, m.ConnectionState)
	}
	return Decoded{}, fmt.Errorf("%w: vda5050 kind %d", ErrMalformed, kind)
}

func (m VDA5050State) toSample(receivedAt time.Time) fleet.Sample {
	s := fleet.Sample{
		DeviceTime: m.Timestamp,
		ReceivedAt: receivedAt,
		Battery:    fleet.Battery{Percent: m.BatteryState.BatteryCharge, Charging: m.BatteryState.Charging},
	}
	if p := m.AGVPosition; p != nil && p.PositionInitialized {
		s.Pose = fleet.Pose{MapID: p.MapID, X: p.X, Y: p.Y, Heading: p.Theta}
	}
	fatal := false
	for _, e := range m.Errors {
		sev := fleet.SeverityWarning
		if e.ErrorLevel == "FATAL" {
			sev = fleet.SeverityCritical
			fatal = true
		}
		s.Faults = append(s.Faults, fleet.Fault{Code: e.ErrorType, Severity: sev, Message: e.ErrorDescription})
	}

	switch {
	case fatal:
		s.Mode = fleet.ModeError
	case m.OperatingMode == "MANUAL" || m.OperatingMode == "SERVICE" || m.OperatingMode == "TEACHIN":
		s.Mode = fleet.ModeManual
	case m.BatteryState.Charging:
		s.Mode = fleet.ModeCharging
	case m.Driving:
		s.Mode = fleet.ModeWorking
	default:
		s.Mode = fleet.ModeIdle
	}
	return s
}
