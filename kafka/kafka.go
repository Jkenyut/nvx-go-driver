package kafka

import (
	"context"
	"crypto/tls"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/Jkenyut/nvx-go-driver/config"
	"github.com/rs/zerolog"
)

// Client acts as a factory for creating Producers and Consumers
// with consistent configuration and logging.
type Client struct {
	cfg        config.KafkaConfig
	saramaConf *sarama.Config
	log        *zerolog.Logger
	brokers    []string

	// Internal singleton producer for shortcuts
	producer sarama.SyncProducer
	prodLock sync.Mutex
}

// NewClient creates a new Kafka factory.
// It validates connection by initializing a temporary client to the broker.
//
// Defaults applied:
//   - Host: "127.0.0.1:9092" (if empty)
//   - SecurityProtocol: "SASL_SSL" (if username set but protocol empty)
//   - Mechanism: "PLAIN"
//
// Usage Example:
//
//	cfg := config.KafkaConfig{
//	    Enable: true,
//	    Host:   "localhost:9092",
//	    Username: "user",
//	    Password: "password",
//	}
//	kafkaFactory, err := kafka.NewClient(cfg, logger)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// Create a producer
//	producer, _ := kafkaFactory.NewProducer()
func NewClient(cfg config.KafkaConfig, logger *zerolog.Logger) (*Client, error) {
	if logger == nil {
		nop := zerolog.Nop()
		logger = &nop
	}

	if !cfg.Enable {
		return nil, errors.New("kafka disabled in config")
	}

	cfg = applyDefaults(cfg)

	conf := sarama.NewConfig()
	conf.Version = sarama.V2_8_0_0 // Safe default

	// Network
	if cfg.SecurityProtocol == "SASL_SSL" || cfg.SecurityProtocol == "SSL" {
		conf.Net.TLS.Enable = true
		conf.Net.TLS.Config = &tls.Config{
			InsecureSkipVerify: true, // Common for internal clusters
		}
	}

	// SASL Auth
	if cfg.Username != "" {
		conf.Net.SASL.Enable = true
		conf.Net.SASL.User = cfg.Username
		conf.Net.SASL.Password = cfg.Password

		// Default to PLAIN mechanism as it is most common and doesn't require extra libs
		conf.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	}

	// Producer reliability
	conf.Producer.Return.Successes = true
	conf.Producer.RequiredAcks = sarama.WaitForAll
	conf.Producer.Retry.Max = 5
	conf.Producer.Retry.Backoff = 100 * time.Millisecond

	brokerList := strings.Split(cfg.Host, ",")

	// Fast fail check
	client, err := sarama.NewClient(brokerList, conf)
	if err != nil {
		return nil, err
	}
	defer client.Close() // We only close the verification client

	logger.Info().Str("brokers", cfg.Host).Msg("Kafka config valid and reachable")

	return &Client{
		cfg:        cfg,
		saramaConf: conf,
		log:        logger,
		brokers:    brokerList,
	}, nil
}

func applyDefaults(cfg config.KafkaConfig) config.KafkaConfig {
	if cfg.Host == "" {
		cfg.Host = "127.0.0.1:9092"
	}
	// If auth is provided but protocol missing, prefer secure
	if cfg.Username != "" && cfg.SecurityProtocol == "" {
		cfg.SecurityProtocol = "SASL_SSL"
	}
	// If simple local
	if cfg.Username == "" && cfg.SecurityProtocol == "" {
		cfg.SecurityProtocol = "PLAINTEXT"
	}
	return cfg
}

// NewProducer creates a synchronous producer.
// SyncProducer publishes messages and waits for acknowledgement (ACK).
func (k *Client) NewProducer() (sarama.SyncProducer, error) {
	return sarama.NewSyncProducer(k.brokers, k.saramaConf)
}

// NewAsyncProducer creates an asynchronous producer.
// AsyncProducer publishes messages to a channel and does not wait for ACK immediately,
// increasing throughput.
func (k *Client) NewAsyncProducer() (sarama.AsyncProducer, error) {
	return sarama.NewAsyncProducer(k.brokers, k.saramaConf)
}

// NewConsumerGroup creates a consumer group.
// It manages partition offsets and rebalancing automatically.
func (k *Client) NewConsumerGroup(groupID string) (sarama.ConsumerGroup, error) {
	return sarama.NewConsumerGroup(k.brokers, groupID, k.saramaConf)
}

// Publish sends a message to the specified topic.
// It uses an internal synchronous producer (singleton) to ensure reliability.
// This is a shortcut for creating a NewProducer() and sending a message.
func (k *Client) Publish(ctx context.Context, topic string, value []byte) error {
	k.prodLock.Lock()
	defer k.prodLock.Unlock()

	if k.producer == nil {
		p, err := sarama.NewSyncProducer(k.brokers, k.saramaConf)
		if err != nil {
			return err
		}
		k.producer = p
	}

	_, _, err := k.producer.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(value),
	})
	return err
}

// Close closes the internal producer if it was initialized.
// Note: Created factories (NewProducer, etc) must be closed individually by the caller.
func (k *Client) Close() error {
	k.prodLock.Lock()
	defer k.prodLock.Unlock()

	if k.producer != nil {
		err := k.producer.Close()
		k.producer = nil
		return err
	}
	return nil
}
