package rabbitmq

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Jkenyut/nvx-go-driver/config"
	driverLogger "github.com/Jkenyut/nvx-go-driver/logger"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog"
)

// Client represents a RabbitMQ client.
type Client struct {
	cfg *config.RabbitMQConfig
	log *zerolog.Logger

	lock sync.RWMutex

	conn *amqp.Connection

	closed atomic.Bool
	ready  atomic.Bool

	done chan struct{}
	wg   sync.WaitGroup
}

// NewClient creates a new RabbitMQ client.
func NewClient(
	cfg *config.RabbitMQConfig,
	logger *zerolog.Logger,
) (*Client, error) {
	if logger == nil {
		logger = driverLogger.L()
	}

	if !cfg.Enable {
		return nil, errors.New(
			"rabbitmq disabled in config",
		)
	}

	cfg = cfg.WithDefaults()

	client := &Client{
		cfg:  cfg,
		log:  logger,
		done: make(chan struct{}),
	}

	err := client.connect()
	if err != nil {
		return nil, err
	}

	client.wg.Add(1)

	go func() {
		defer client.wg.Done()
		client.reconnectLoop()
	}()

	return client, nil
}

func (r *Client) connectTimeout() time.Duration {
	if r.cfg.ConnectTimeout <= 0 {
		return 10 * time.Second
	}
	return time.Duration(r.cfg.ConnectTimeout) * time.Second
}

func (r *Client) publishTimeout() time.Duration {
	if r.cfg.PublishTimeout <= 0 {
		return 5 * time.Second
	}
	return time.Duration(r.cfg.PublishTimeout) * time.Second
}

func (r *Client) connect() error {
	scheme := "amqp"
	if r.cfg.TLS {
		scheme = "amqps"
	}
	dsn := (&url.URL{
		Scheme: scheme,
		User:   url.UserPassword(r.cfg.Username, r.cfg.Password),
		Host:   net.JoinHostPort(r.cfg.Host, fmt.Sprintf("%d", r.cfg.Port)),
		Path:   "/",
	}).String()

	amqpConfig := amqp.Config{
		Heartbeat: 10 * time.Second,
		Locale:    "en_US",
		Dial:      amqp.DefaultDial(r.connectTimeout()),
	}
	if r.cfg.TLS {
		amqpConfig.TLSClientConfig = &tls.Config{
			ServerName:         r.cfg.Host,
			InsecureSkipVerify: r.cfg.InsecureSkipVerify,
		}
	}

	conn, err := amqp.DialConfig(
		dsn,
		amqpConfig,
	)
	if err != nil {
		r.ready.Store(false)

		return fmt.Errorf(
			"dial rabbitmq: %w",
			err,
		)
	}

	r.lock.Lock()

	oldConn := r.conn
	r.conn = conn

	r.lock.Unlock()

	if oldConn != nil && !oldConn.IsClosed() {
		_ = oldConn.Close()
	}

	r.ready.Store(true)

	r.log.Info().
		Str("host", r.cfg.Host).
		Int("port", r.cfg.Port).
		Msg("RabbitMQ connected")

	return nil
}

func (r *Client) reconnectDuration() time.Duration {
	if r.cfg.ReconnectDuration <= 0 {
		return time.Second
	}
	return time.Duration(r.cfg.ReconnectDuration) * time.Second
}

func (r *Client) reconnectLoop() {
	backoff := r.reconnectDuration()
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
			select {
			case <-r.done:
				return
			case <-time.After(backoff):
			}

			continue
		}

		closeChan := conn.NotifyClose(
			make(chan *amqp.Error, 1),
		)

		err := <-closeChan

		select {
		case <-r.done:
			return
		default:
		}

		// connection already replaced
		r.lock.RLock()
		currentConn := r.conn
		r.lock.RUnlock()

		if currentConn != conn {
			continue
		}

		if r.closed.Load() {
			return
		}

		r.ready.Store(false)

		if err != nil {
			r.log.Warn().
				Err(err).
				Msg("RabbitMQ connection lost")
		} else {
			r.log.Warn().
				Msg("RabbitMQ connection closed unexpectedly")
		}

		for {
			select {
			case <-r.done:
				return
			default:
			}

			errs := r.connect()
			if errs == nil {
				backoff = r.reconnectDuration()

				r.log.Info().
					Msg("RabbitMQ reconnected")

				break
			}

			r.log.Error().
				Err(errs).
				Msg("RabbitMQ reconnect failed")

			select {
			case <-r.done:
				return
			case <-time.After(backoff):
			}

			if backoff < 30*time.Second {
				backoff *= 2
			}
		}
	}
}

// IsReady returns true if the client is ready.
func (r *Client) IsReady() bool {
	return r.ready.Load()
}

// Connection returns the underlying amqp.Connection.
func (r *Client) Connection() (*amqp.Connection, error) {
	r.lock.RLock()

	conn := r.conn

	r.lock.RUnlock()

	if conn == nil || conn.IsClosed() {
		return nil, errors.New(
			"rabbitmq connection unavailable",
		)
	}

	return conn, nil
}

// NewChannel creates and returns a new amqp.Channel.
func (r *Client) NewChannel() (*amqp.Channel, error) {
	conn, err := r.Connection()
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf(
			"create rabbitmq channel: %w",
			err,
		)
	}

	return ch, nil
}

// Ping verifies if the connection is alive.
func (r *Client) Ping() error {
	ch, err := r.NewChannel()
	if err != nil {
		return err
	}

	defer func() {
		if !ch.IsClosed() {
			_ = ch.Close()
		}
	}()

	return nil
}

// Close gracefully closes the client.
func (r *Client) Close() error {
	if !r.closed.CompareAndSwap(false, true) {
		return nil
	}

	select {
	case <-r.done:
	default:
		close(r.done)
	}

	r.lock.Lock()

	conn := r.conn
	r.conn = nil

	r.lock.Unlock()

	if conn != nil && !conn.IsClosed() {
		_ = conn.Close()
	}

	r.wg.Wait()

	r.ready.Store(false)

	return nil
}

// DeclareExchange creates an exchange on the RabbitMQ server.
func (r *Client) DeclareExchange(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
	ch, err := r.NewChannel()
	if err != nil {
		return err
	}
	defer func() { _ = ch.Close() }()

	return ch.ExchangeDeclare(name, kind, durable, autoDelete, internal, noWait, args)
}

// DeclareQueue creates a queue on the RabbitMQ server.
func (r *Client) DeclareQueue(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	ch, err := r.NewChannel()
	if err != nil {
		return amqp.Queue{}, err
	}
	defer func() { _ = ch.Close() }()

	return ch.QueueDeclare(name, durable, autoDelete, exclusive, noWait, args)
}

// BindQueue binds a queue to an exchange with a routing key.
func (r *Client) BindQueue(queue, routingKey, exchange string, noWait bool, args amqp.Table) error {
	ch, err := r.NewChannel()
	if err != nil {
		return err
	}
	defer func() { _ = ch.Close() }()

	return ch.QueueBind(queue, routingKey, exchange, noWait, args)
}
