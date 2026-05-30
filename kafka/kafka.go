// Package kafka provides a Kafka client implementation.
package kafka

import (
	"context"
	"crypto/tls"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Jkenyut/nvx-go-driver/config"
	"github.com/rs/zerolog"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
)

// ErrClientClosed is returned when an operation is attempted on a closed client.
var ErrClientClosed = errors.New("kafka client is closed")

// SmartBalancer balances keyed messages by Murmur2 (Java compatible),
// and keyless by LeastBytes to prevent partition 0 hotspotting.
type SmartBalancer struct {
	Murmur2    kafka.Murmur2Balancer
	LeastBytes kafka.LeastBytes
}

// Balance returns the partition for the given message.
func (b *SmartBalancer) Balance(msg kafka.Message, partitions ...int) int {
	if len(msg.Key) > 0 {
		return b.Murmur2.Balance(msg, partitions...)
	}
	return b.LeastBytes.Balance(msg, partitions...)
}

// Client acts as a factory for creating Producers (Writers) and Consumers (Readers)
// with consistent configuration and logging.
type Client struct {
	cfg     *config.KafkaConfig
	dialer  *kafka.Dialer
	log     *zerolog.Logger
	brokers []string

	// Internal singleton producer for shortcuts
	writer     *kafka.Writer
	writerLock sync.RWMutex
	closed     atomic.Uint32
}

// NewClient creates a new Kafka factory based on segmentio/kafka-go.
// It validates connection by dialing the first broker.
//
// Defaults applied:
//   - Host: "127.0.0.1:9092" (if empty)
//   - SecurityProtocol: "SASL_SSL" (if username set but protocol empty)
//   - Mechanism: "PLAIN"
func NewClient(cfg *config.KafkaConfig, logger *zerolog.Logger) (*Client, error) {
	if logger == nil {
		nop := zerolog.Nop()
		logger = &nop
	}

	if !cfg.Enable {
		return nil, errors.New("kafka disabled in config")
	}

	cfg = cfg.WithDefaults()

	var mechanism sasl.Mechanism
	if cfg.Username != "" {
		switch strings.ToUpper(cfg.Mechanisms) {
		case "SCRAM-SHA-256":
			mech, err := scram.Mechanism(scram.SHA256, cfg.Username, cfg.Password)
			if err != nil {
				return nil, err
			}
			mechanism = mech
		case "SCRAM-SHA-512":
			mech, err := scram.Mechanism(scram.SHA512, cfg.Username, cfg.Password)
			if err != nil {
				return nil, err
			}
			mechanism = mech
		default: // Defaults to PLAIN
			mechanism = plain.Mechanism{
				Username: cfg.Username,
				Password: cfg.Password,
			}
		}
	}

	var tlsConf *tls.Config
	if cfg.SecurityProtocol == "SASL_SSL" || cfg.SecurityProtocol == "SSL" {
		tlsConf = &tls.Config{
			InsecureSkipVerify: cfg.InsecureSkipVerify,
		}
	}

	dialer := &kafka.Dialer{
		Timeout:       10 * time.Second,
		DualStack:     true,
		TLS:           tlsConf,
		SASLMechanism: mechanism,
	}

	brokerList := strings.Split(cfg.Host, ",")

	// Fast fail check - try all brokers, succeed if at least one is reachable
	var lastErr error
	var connected bool
	for _, broker := range brokerList {
		conn, err := dialer.DialContext(context.Background(), "tcp", strings.TrimSpace(broker))
		if err == nil {
			_ = conn.Close()
			connected = true
			break
		}
		lastErr = err
	}

	if !connected {
		if lastErr != nil {
			return nil, errors.New("all kafka brokers unreachable: " + lastErr.Error())
		}
		return nil, errors.New("no kafka brokers configured")
	}

	logger.Info().Str("brokers", cfg.Host).Msg("Kafka config valid and reachable")

	return &Client{
		cfg:     cfg,
		dialer:  dialer,
		log:     logger,
		brokers: brokerList,
	}, nil
}

// NewWriter creates a new Kafka Writer (Producer).
// Writers in kafka-go automatically handle retries, connection drops, and load balancing.
func (k *Client) NewWriter() *kafka.Writer {
	return &kafka.Writer{
		Addr: kafka.TCP(k.brokers...),
		Transport: &kafka.Transport{
			TLS:         k.dialer.TLS,
			SASL:        k.dialer.SASLMechanism,
			DialTimeout: 10 * time.Second,
			IdleTimeout: 9 * time.Minute,
		},
		Balancer:     &SmartBalancer{},
		RequiredAcks: kafka.RequireAll,
		MaxAttempts:  5,
		BatchTimeout: 10 * time.Millisecond,
		BatchSize:    100,
	}
}

// NewReader creates a new Kafka Reader (Consumer Group).
// Readers automatically handle rebalancing and offset commits.
// Usage: msg, err := reader.ReadMessage(ctx)
func (k *Client) NewReader(topic, groupID string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers: k.brokers,
		GroupID: groupID,
		Topic:   topic,
		Dialer:  k.dialer,
		MaxWait: 1 * time.Second, // Batching wait time
	})
}

// Publish is a convenient shortcut to send a single message synchronously.
// It accepts a 'key' for partition ordering guarantees.
func (k *Client) Publish(ctx context.Context, topic string, key, value []byte) error {
	if k.closed.Load() == 1 {
		return ErrClientClosed
	}

	k.writerLock.RLock()
	w := k.writer
	k.writerLock.RUnlock()

	if w == nil {
		k.writerLock.Lock()
		if k.closed.Load() == 1 {
			k.writerLock.Unlock()
			return ErrClientClosed
		}
		if k.writer == nil {
			k.writer = k.NewWriter()
		}
		w = k.writer
		k.writerLock.Unlock()
	}

	msg := kafka.Message{
		Topic: topic,
		Value: value,
	}
	if len(key) > 0 {
		msg.Key = key
	}

	// WriteMessages blocks until the message is written or the context is cancelled.
	return w.WriteMessages(ctx, msg)
}

// Close gracefully flushes and closes the internal singleton writer if initialized.
func (k *Client) Close() error {
	if !k.closed.CompareAndSwap(0, 1) {
		return nil
	}

	k.writerLock.Lock()
	defer k.writerLock.Unlock()

	if k.writer != nil {
		err := k.writer.Close()
		k.writer = nil
		return err
	}
	return nil
}
