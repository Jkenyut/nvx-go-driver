package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Action int

const (
	ActionAck Action = iota
	ActionNackRequeue
	ActionNackDiscard
)

type HandlerFunc func(
	ctx context.Context,
	msg amqp.Delivery,
) Action

type Consumer struct {
	client *Client

	queue string

	autoAck bool
	qos     int

	lock sync.RWMutex

	ch *amqp.Channel

	done      chan struct{}
	wg        sync.WaitGroup
	exclusive bool
	noLocal   bool
	noWait    bool
	args      amqp.Table
}

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

func (c *Consumer) SetQos(
	qos int,
) {
	c.qos = qos
}

func (c *Consumer) SetAutoAck(
	autoAck bool,
) {
	c.autoAck = autoAck
}

func (c *Consumer) SetExclusive(
	exclusive bool,
) {
	c.exclusive = exclusive
}

func (c *Consumer) SetNoLocal(
	noLocal bool,
) {
	c.noLocal = noLocal
}

func (c *Consumer) SetNoWait(
	noWait bool,
) {
	c.noWait = noWait
}

func (c *Consumer) SetArgs(
	args amqp.Table,
) {
	c.args = args
}

func (c *Consumer) Start(
	ctx context.Context,
	handler HandlerFunc,
) {

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
				Str("queue", c.queue).
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
		c.lock.Unlock()

		if !ch.IsClosed() {
			_ = ch.Close()
		}
	}()

	err = ch.Qos(
		c.qos,
		0,
		false,
	)

	if err != nil {
		return err
	}

	consumerTag := fmt.Sprintf(
		"%s-%d",
		c.queue,
		time.Now().UnixNano(),
	)

	msgs, err := ch.Consume(
		c.queue,
		consumerTag,
		c.autoAck,
		c.exclusive,
		c.noLocal,
		c.noWait,
		c.args,
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

		case <-c.done:
			return nil

		case msg, ok := <-msgs:

			if !ok {
				return errors.New(
					"delivery channel closed",
				)
			}

			func() {

				defer func() {

					if recover() != nil {

						c.client.log.Error().
							Str("queue", c.queue).
							Msg("Consumer panic recovered")

						if !c.autoAck {
							_ = msg.Nack(false, true)
						}
					}
				}()

				action := handler(ctx, msg)

				if !c.autoAck {
					switch action {
					case ActionAck:
						_ = msg.Ack(false)
					case ActionNackRequeue:
						_ = msg.Nack(false, true)
					case ActionNackDiscard:
						_ = msg.Nack(false, false)
					default:
						// Safe fallback: Discard on unknown action
						c.client.log.Warn().
							Int("action", int(action)).
							Str("queue", c.queue).
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

func (c *Consumer) Close() {

	select {
	case <-c.done:
	default:
		close(c.done)
	}

	c.lock.RLock()

	ch := c.ch

	c.lock.RUnlock()

	if ch != nil && !ch.IsClosed() {
		_ = ch.Close()
	}

	c.wg.Wait()
}
