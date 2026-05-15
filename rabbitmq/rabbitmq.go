package rabbitmq

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Jkenyut/nvx-go-driver/config"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog"
)

type Client struct {
	cfg config.RabbitMQConfig
	log *zerolog.Logger

	lock sync.RWMutex

	conn *amqp.Connection

	closed atomic.Bool
	ready  atomic.Bool

	done chan struct{}
	wg   sync.WaitGroup
}

func NewClient(
	cfg config.RabbitMQConfig,
	logger *zerolog.Logger,
) (*Client, error) {

	if logger == nil {
		nop := zerolog.Nop()
		logger = &nop
	}

	if !cfg.Enable {
		return nil, errors.New(
			"rabbitmq disabled in config",
		)
	}

	cfg = applyDefaults(cfg)

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

func applyDefaults(
	cfg config.RabbitMQConfig,
) config.RabbitMQConfig {

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

	dsn := fmt.Sprintf(
		"amqp://%s:%s@%s:%d/",
		r.cfg.Username,
		r.cfg.Password,
		r.cfg.Host,
		r.cfg.Port,
	)

	conn, err := amqp.DialConfig(
		dsn,
		amqp.Config{
			Heartbeat: 10 * time.Second,
			Locale:    "en_US",
		},
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

func (r *Client) reconnectLoop() {
	backoff := time.Second
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

		err, ok := <-closeChan

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

		// graceful close
		if !ok || err == nil {

			if r.closed.Load() {
				return
			}

			select {
			case <-r.done:
				return
			case <-time.After(backoff):
			}

			continue
		}

		r.ready.Store(false)

		r.log.Warn().
			Err(err).
			Msg("RabbitMQ connection lost")

		for {

			select {
			case <-r.done:
				return
			default:
			}

			errs := r.connect()
			if errs == nil {

				backoff = time.Second

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

func (r *Client) IsReady() bool {
	return r.ready.Load()
}

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

func (r *Client) Close() error {

	if r.closed.Load() {
		return nil
	}

	r.closed.Store(true)

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
	defer ch.Close()

	return ch.ExchangeDeclare(name, kind, durable, autoDelete, internal, noWait, args)
}

// DeclareQueue creates a queue on the RabbitMQ server.
func (r *Client) DeclareQueue(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	ch, err := r.NewChannel()
	if err != nil {
		return amqp.Queue{}, err
	}
	defer ch.Close()

	return ch.QueueDeclare(name, durable, autoDelete, exclusive, noWait, args)
}

// BindQueue binds a queue to an exchange with a routing key.
func (r *Client) BindQueue(queue, routingKey, exchange string, noWait bool, args amqp.Table) error {
	ch, err := r.NewChannel()
	if err != nil {
		return err
	}
	defer ch.Close()

	return ch.QueueBind(queue, routingKey, exchange, noWait, args)
}
