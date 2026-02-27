package rabbitmq

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Jkenyut/nvx-go-driver/config"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog"
)

// Client handles connection to RabbitMQ with auto-reconnect,
// thread-safe channel access, and graceful shutdown.
type Client struct {
	cfg  config.RabbitMQConfig
	log  *zerolog.Logger
	lock sync.RWMutex

	conn    *amqp.Connection
	channel *amqp.Channel
	closed  bool
	done    chan struct{}
}

// NewClient creates a new RabbitMQ client and starts the reconnect monitor.
// It applies sensible defaults for missing configuration values.
//
// Defaults applied:
//   - Host: "127.0.0.1" (if empty)
//   - Port: 5672 (if 0)
//   - Username: "guest" (if empty)
//   - Password: "guest" (if empty)
//   - ReconnectDuration: 5s (if 0)
//
// Usage Example:
//
//	cfg := config.RabbitMQConfig{
//	    Enable: true,
//	    Host:   "localhost",
//	}
//	mq, err := rabbitmq.NewClient(cfg, logger)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer mq.Close()
//
//	// Get a channel to publish/consume
//	ch, err := mq.Channel()
//	if err == nil {
//	    ch.Publish(...)
//	}
func NewClient(cfg config.RabbitMQConfig, logger *zerolog.Logger) (*Client, error) {
	if logger == nil {
		nop := zerolog.Nop()
		logger = &nop
	}

	if !cfg.Enable {
		return nil, errors.New("rabbitmq disabled in config")
	}

	cfg = applyDefaults(cfg)

	client := &Client{
		cfg:  cfg,
		log:  logger,
		done: make(chan struct{}),
	}

	if err := client.connect(); err != nil {
		return nil, err
	}

	go client.reconnectLoop()

	return client, nil
}

func applyDefaults(cfg config.RabbitMQConfig) config.RabbitMQConfig {
	if cfg.Host == "" {
		cfg.Host = "127.0.0.1"
	}
	if cfg.Port == 0 {
		cfg.Port = 5672
	}
	if cfg.Username == "" {
		cfg.Username = "guest"
	}
	if cfg.Password == "" {
		cfg.Password = "guest"
	}
	if cfg.ReconnectDuration == 0 {
		cfg.ReconnectDuration = 5
	}
	return cfg
}

func (r *Client) connect() error {
	r.lock.Lock()
	defer r.lock.Unlock()

	dsn := fmt.Sprintf("amqp://%s:%s@%s:%d/",
		r.cfg.Username,
		r.cfg.Password,
		r.cfg.Host,
		r.cfg.Port,
	)

	conn, err := amqp.Dial(dsn)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return fmt.Errorf("channel: %w", err)
	}

	r.conn = conn
	r.channel = ch
	r.log.Info().Str("host", r.cfg.Host).Msg("RabbitMQ connected")

	return nil
}

func (r *Client) reconnectLoop() {
	for {
		select {
		case <-r.done:
			return
		default:
		}

		r.lock.RLock()
		conn := r.conn
		r.lock.RUnlock()

		if conn == nil {
			time.Sleep(time.Duration(r.cfg.ReconnectDuration) * time.Second)
			continue
		}

		reason, ok := <-conn.NotifyClose(make(chan *amqp.Error))
		if !ok {
			// Normal closure (triggered by Close()) or channel closed
			// If r.closed is true, we exit naturally
			if r.isClosed() {
				return
			}
		} else {
			r.log.Warn().Err(reason).Msg("RabbitMQ connection lost, reconnecting...")
		}

		for {
			if r.isClosed() {
				return
			}

			time.Sleep(time.Duration(r.cfg.ReconnectDuration) * time.Second)

			if err := r.connect(); err == nil {
				r.log.Info().Msg("RabbitMQ reconnected")
				break
			} else {
				r.log.Error().Err(err).Msg("Failed to reconnect RabbitMQ, retrying...")
			}
		}
	}
}

func (r *Client) isClosed() bool {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return r.closed
}

// GetChannel returns the current active channel safely.
// Note: You must handle the error if the channel is currently disconnected.
// The returned channel is thread-safe for most operations, but standard AMQP
// protocol rules apply (don't share channels across threads for publishing if execution order matters significantly,
// though standard usage is often fine).
func (r *Client) GetChannel() (*amqp.Channel, error) {
	ch, err := r.Channel()
	if err != nil {
		return nil, err
	}
	return ch, nil
}

// Channel returns the current active channel safely.
// Note: You must handle the error if the channel is currently disconnected.
// The returned channel is thread-safe for most operations, but standard AMQP
// protocol rules apply (don't share channels across threads for publishing if execution order matters significantly,
// though standard usage is often fine).
func (r *Client) Channel() (*amqp.Channel, error) {
	r.lock.RLock()
	defer r.lock.RUnlock()
	if r.channel == nil || r.channel.IsClosed() {
		return nil, errors.New("rabbitmq channel is closed or reconnecting")
	}
	return r.channel, nil
}

// Close gracefully shuts down the client and the reconnect monitor.
func (r *Client) Close() error {
	r.lock.Lock()
	defer r.lock.Unlock()

	if r.closed {
		return nil
	}
	r.closed = true
	close(r.done)

	if r.channel != nil {
		r.channel.Close()
	}
	if r.conn != nil {
		return r.conn.Close()
	}
	return nil
}
