// Package kafka writes frames straight to each exchange's raw topic, bypassing NiFi. The
// payload is shaped by the exchange (its Envelope adds what NiFi would have added); this
// package only sends it.
//
// It never creates a topic — in lpa-core only warmup and lpa-console do — so a missing
// topic stops the tool at startup instead of being created with the broker's defaults.
package kafka

import (
	"context"
	"crypto/rand"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Producer is one long-lived client for the whole run, shared by every exchange.
type Producer struct {
	cl      *kgo.Client
	onError func(topic string)
	log     *slog.Logger
}

// New connects and checks every topic exists. onError is called for every record Kafka
// refuses, with its topic.
func New(ctx context.Context, brokers, topics []string, onError func(topic string), log *slog.Logger) (*Producer, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return nil, fmt.Errorf("kafka client: %w", err)
	}
	checkCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	found, err := kadm.NewClient(cl).ListTopics(checkCtx, topics...)
	if err != nil {
		cl.Close()
		return nil, fmt.Errorf("cannot reach kafka at %v: %w", brokers, err)
	}
	for _, t := range topics {
		if d, ok := found[t]; !ok || d.Err != nil {
			cl.Close()
			return nil, fmt.Errorf("topic %q does not exist: create it with warmup or lpa-console (this tool does not create topics)", t)
		}
	}
	return &Producer{cl: cl, onError: onError, log: log}, nil
}

// Publish sends one record. It blocks while the client's buffer is full, which is how a
// slow Kafka slows the generator down.
func (p *Producer) Publish(topic string, value []byte) {
	p.cl.Produce(context.Background(), &kgo.Record{Topic: topic, Value: value}, func(_ *kgo.Record, err error) {
		if err != nil {
			p.onError(topic)
			p.log.Debug("kafka produce failed", "topic", topic, "err", err)
		}
	})
}

// Close sends what is still buffered, then disconnects.
func (p *Producer) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := p.cl.Flush(ctx); err != nil {
		p.log.Warn("kafka flush", "err", err)
	}
	p.cl.Close()
}

// NewID is a random (version 4) UUID, the kind NiFi's ${uuid} gives.
func NewID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
