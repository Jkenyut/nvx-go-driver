// Package rabbitmq provides a RabbitMQ client implementation.
package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Action represents a consumer action.
type Action int

const (
	// ActionAck represents an ack action.
	ActionAck Action = iota
	// ActionNackRequeue represents a nack and requeue action.
	ActionNackRequeue
	// ActionNackDiscard represents a nack and discard action.
	ActionNackDiscard
	// ActionNone represents no action.
	ActionNone
)

// HandlerFunc is the consumer handler.
type HandlerFunc func(
	ctx context.Context,
	msg amqp.Delivery,
) Action

// Consumer represents a RabbitMQ consumer.
type Consumer struct {
	client *Client

	queue string

	autoAck bool
	qos     int

	lock sync.RWMutex

	ch *amqp.Channel

	consumerTag string
	activeMsgs  atomic.Int64

	done      chan struct{}
	wg        sync.WaitGroup
	started   atomic.Bool
	exclusive bool
	noLocal   bool
	noWait    bool
	args      amqp.Table
}

// NewConsumer creates a new consumer.
func NewConsumer(
	client *Client,
	queue string,
) *Consumer {
	return &Consumer{
		client:  client,
		queue:   queue,
		autoAck: false,
		qos:     1,
		done:    make(chan struct{}),
	}
}

// SetQos sets the QoS.
func (c *Consumer) SetQos(
	qos int,
) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.qos = qos
}

// SetAutoAck sets auto ack.
func (c *Consumer) SetAutoAck(
	autoAck bool,
) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.autoAck = autoAck
}

// SetExclusive sets exclusive mode.
func (c *Consumer) SetExclusive(
	exclusive bool,
) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.exclusive = exclusive
}

// SetNoLocal sets no local.
func (c *Consumer) SetNoLocal(
	noLocal bool,
) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.noLocal = noLocal
}

// SetNoWait sets no wait.
func (c *Consumer) SetNoWait(
	noWait bool,
) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.noWait = noWait
}

// SetArgs sets the arguments.
func (c *Consumer) SetArgs(
	args amqp.Table,
) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.args = args
}

// Start starts the consumer loop.
func (c *Consumer) Start(
	ctx context.Context,
	handler HandlerFunc,
) {
	if !c.started.CompareAndSwap(false, true) {
		if c.client != nil && c.client.log != nil {
			c.client.log.Warn().Str("queue", c.queue).Msg("Consumer Start() ignored: already started")
		}
		return
	}

	c.wg.Add(1)

	go func() {
		defer c.wg.Done()

		c.loop(ctx, handler)
	}()
}

func (c *Consumer) loop(
	ctx context.Context,
	handler HandlerFunc,
) {
	queue := c.queue

	for {
		select {
		case <-ctx.Done():
			return

		case <-c.done:
			return

		default:
		}

		err := c.consume(ctx, handler)
		if err != nil {
			c.client.log.Error().
				Err(err).
				Str("queue", queue).
				Msg("Consumer stopped")
		}

		select {
		case <-ctx.Done():
			return

		case <-c.done:
			return

		case <-time.After(5 * time.Second):
		}
	}
}

func (c *Consumer) consume(
	ctx context.Context,
	handler HandlerFunc,
) error {
	c.lock.RLock()
	queue := c.queue
	autoAck := c.autoAck
	qos := c.qos
	exclusive := c.exclusive
	noLocal := c.noLocal
	noWait := c.noWait
	args := c.args
	c.lock.RUnlock()

	ch, err := c.client.NewChannel()
	if err != nil {
		return err
	}

	c.lock.Lock()
	c.ch = ch
	c.lock.Unlock()

	defer func() {
		c.lock.Lock()
		c.ch = nil
		// Do not clear consumerTag here, as Close() needs it, or just leave it.
		c.lock.Unlock()

		if !ch.IsClosed() {
			_ = ch.Close()
		}
	}()

	err = ch.Qos(
		qos,
		0,
		false,
	)
	if err != nil {
		return err
	}

	consumerTag := fmt.Sprintf(
		"%s-%d",
		queue,
		time.Now().UnixNano(),
	)

	c.lock.Lock()
	c.consumerTag = consumerTag
	c.lock.Unlock()

	msgs, err := ch.Consume(
		queue,
		consumerTag,
		autoAck,
		exclusive,
		noLocal,
		noWait,
		args,
	)
	if err != nil {
		return err
	}

	closeChan := ch.NotifyClose(
		make(chan *amqp.Error, 1),
	)

	for {
		select {
		case <-ctx.Done():
			return nil

		case msg, ok := <-msgs:

			if !ok {
				select {
				case <-c.done:
					return nil
				default:
					return errors.New("delivery channel closed")
				}
			}

			c.activeMsgs.Add(1)
			func() {
				defer c.activeMsgs.Add(-1)

				var span trace.Span
				defer func() {
					if r := recover(); r != nil {
						c.client.log.Error().
							Str("queue", queue).
							Interface("panic", r).
							Msg("Consumer panic recovered")

						if span != nil {
							span.RecordError(fmt.Errorf("consumer panic: %v", r))
						}

						if !autoAck {
							_ = msg.Nack(false, true)
						}
					}
					if span != nil {
						span.End()
					}
				}()

				if c.client.cfg.EnableTelemetry {
					tracer := otel.Tracer("nvx-go-driver/rabbitmq")
					ctx, span = tracer.Start(ctx, "RabbitMQ Consume", trace.WithSpanKind(trace.SpanKindConsumer))
					span.SetAttributes(
						attribute.String("messaging.system", "rabbitmq"),
						attribute.String("messaging.destination", queue),
					)
				}

				action := handler(ctx, msg)

				if !autoAck {
					switch action {
					case ActionAck:
						_ = msg.Ack(false)
					case ActionNackRequeue:
						_ = msg.Nack(false, true)
					case ActionNackDiscard:
						_ = msg.Nack(false, false)
					case ActionNone:
						// User handles Ack/Nack manually
					default:
						// Safe fallback: Discard on unknown action
						c.client.log.Warn().
							Int("action", int(action)).
							Str("queue", queue).
							Msg("Unknown consumer action, discarding message")
						_ = msg.Nack(false, false)
					}
				}
			}()

		case err := <-closeChan:

			if err != nil {
				return err
			}

			return errors.New(
				"consumer channel closed",
			)
		}
	}
}

// Close gracefully closes the consumer.
func (c *Consumer) Close() error {
	c.lock.Lock()

	select {
	case <-c.done:
		c.lock.Unlock()
		return nil
	default:
		close(c.done)
	}

	ch := c.ch
	tag := c.consumerTag
	c.lock.Unlock()

	if ch != nil && !ch.IsClosed() {
		_ = ch.Cancel(tag, false)

		timeout := time.After(10 * time.Second)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

	waitLoop:
		for {
			if c.activeMsgs.Load() == 0 {
				break waitLoop
			}
			select {
			case <-timeout:
				if c.client != nil && c.client.log != nil {
					c.client.log.Warn().Str("queue", c.queue).Msg("Consumer graceful shutdown timeout, forcing channel close")
				}
				break waitLoop
			case <-ticker.C:
			}
		}

		_ = ch.Close()
	}

	waitDone := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(waitDone)
	}()

	select {
	case <-waitDone:
	case <-time.After(2 * time.Second):
		if c.client != nil && c.client.log != nil {
			c.client.log.Error().Str("queue", c.queue).Msg("Consumer Close() forced exit: goroutine leaked due to hung handler")
		}
	}

	return nil
}
