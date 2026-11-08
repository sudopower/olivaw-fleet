// Package mqtt connects the ingest pipeline to an MQTT broker.
package mqtt

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
)

// Handler receives every message. It is called on the client's single
// delivery goroutine, in broker order, so it must not block.
type Handler func(topic string, payload []byte, receivedAt time.Time)

// Subscriber is a connected MQTT client that keeps its subscriptions across
// reconnects.
type Subscriber struct {
	client paho.Client
}

// Subscribe connects to broker and subscribes to topics.
//
// Order matters is left on: messages from one robot must reach the pipeline
// in the order the broker delivered them. Scaling past one client is done by
// partitioning robots across clients (ADR 0005), not by delivering one
// client's messages concurrently.
func Subscribe(ctx context.Context, broker, clientID string, topics []string, h Handler, log *slog.Logger) (*Subscriber, error) {
	filters := make(map[string]byte, len(topics))
	for _, t := range topics {
		filters[t] = 1
	}
	opts := paho.NewClientOptions().
		AddBroker(broker).
		SetClientID(clientID).
		SetCleanSession(true).
		SetOrderMatters(true).
		SetAutoReconnect(true).
		SetMaxReconnectInterval(10 * time.Second).
		SetConnectionLostHandler(func(_ paho.Client, err error) {
			log.Warn("mqtt connection lost", "err", err)
		}).
		// Subscriptions are (re)made on every connect, because a clean
		// session forgets them when the connection drops.
		SetOnConnectHandler(func(c paho.Client) {
			tok := c.SubscribeMultiple(filters, func(_ paho.Client, m paho.Message) {
				h(m.Topic(), m.Payload(), time.Now())
			})
			if tok.Wait() && tok.Error() != nil {
				log.Error("mqtt subscribe failed", "err", tok.Error())
				return
			}
			log.Info("mqtt subscribed", "broker", broker, "topics", topics)
		})

	client := paho.NewClient(opts)
	tok := client.Connect()
	select {
	case <-tok.Done():
		if err := tok.Error(); err != nil {
			return nil, fmt.Errorf("mqtt connect %s: %w", broker, err)
		}
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return &Subscriber{client: client}, nil
}

// Close disconnects, giving in-flight work a moment to finish.
func (s *Subscriber) Close() { s.client.Disconnect(500) }
