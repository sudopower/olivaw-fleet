// Package telemetry turns vendor wire formats into fleet.Sample.
//
// Every manufacturer speaks its own protocol. An Adapter owns one of them:
// which MQTT topics it uses, how a topic identifies a device, and how a
// payload maps onto the domain model. Adding a manufacturer means adding an
// adapter; nothing downstream changes. Only VDA 5050 is implemented here.
//
// Routing and decoding are split: routing only parses the topic, so a
// message for an unknown robot is rejected before any JSON is parsed.
package telemetry

import (
	"errors"
	"time"

	"github.com/sudopower/olivaw-fleet/internal/fleet"
)

// Device is how a robot identifies itself on the wire. The registry maps it
// to a platform RobotID.
type Device struct {
	Manufacturer string
	Serial       string
}

// Kind is the type of message on a topic.
type Kind int

const (
	KindState      Kind = iota + 1 // periodic telemetry
	KindConnection                 // connection state, incl. MQTT last-will
)

// Decoded is the result of decoding one payload.
type Decoded struct {
	// Sample is set for KindState. Its RobotID is empty: identity is resolved
	// by the registry, not trusted from the payload.
	Sample fleet.Sample

	// Disconnected is set for KindConnection when the robot's connection is
	// gone. A "connected" message is ignored; the next sample says more.
	Disconnected bool
}

// Adapter handles one vendor protocol.
type Adapter interface {
	// Subscriptions are the MQTT topic filters this adapter needs.
	Subscriptions() []string
	// Route identifies the device and message kind from a topic.
	Route(topic string) (Device, Kind, bool)
	// Decode parses a payload. receivedAt is the server time of arrival.
	Decode(kind Kind, payload []byte, receivedAt time.Time) (Decoded, error)
}

// ErrMalformed is wrapped by every decode failure.
var ErrMalformed = errors.New("malformed payload")

// Envelope is a routed but not yet decoded message.
type Envelope struct {
	Device     Device
	Kind       Kind
	Payload    []byte
	ReceivedAt time.Time

	adapter Adapter
}

// Decode parses the payload with the adapter that routed it.
func (e Envelope) Decode() (Decoded, error) {
	return e.adapter.Decode(e.Kind, e.Payload, e.ReceivedAt)
}

// Router dispatches topics to the adapter that owns them.
type Router struct {
	adapters []Adapter
}

// NewRouter returns a router over the given adapters.
func NewRouter(adapters ...Adapter) *Router {
	return &Router{adapters: adapters}
}

// Subscriptions lists every topic filter of every adapter.
func (r *Router) Subscriptions() []string {
	var subs []string
	for _, a := range r.adapters {
		subs = append(subs, a.Subscriptions()...)
	}
	return subs
}

// Route finds the adapter for topic. ok is false for topics no adapter owns.
func (r *Router) Route(topic string, payload []byte, receivedAt time.Time) (Envelope, bool) {
	for _, a := range r.adapters {
		if dev, kind, ok := a.Route(topic); ok {
			return Envelope{Device: dev, Kind: kind, Payload: payload, ReceivedAt: receivedAt, adapter: a}, true
		}
	}
	return Envelope{}, false
}
