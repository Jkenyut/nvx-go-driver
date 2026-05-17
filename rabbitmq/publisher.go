package rabbitmq

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Publisher struct {
	client *Client

	lock sync.RWMutex

	ch *amqp.Channel

	publishLock     sync.Mutex
	pendingConfirms sync.Map

	maxAttempts int

	done chan struct{}
	wg   sync.WaitGroup
}

func NewPublisher(
	client *Client,
) (*Publisher, error) {

	ch, err := client.NewChannel()
	if err != nil {
		return nil, err
	}

	err = ch.Confirm(false)
	if err != nil {
		_ = ch.Close()
		return nil, err
	}

	p := &Publisher{
		client:      client,
		ch:          ch,
		done:        make(chan struct{}),
		maxAttempts: 3,
	}

	p.startConfirmListener(ch)

	p.wg.Add(1)

	go func() {
		defer p.wg.Done()
		p.reconnectLoop()
	}()

	return p, nil
}

// SetMaxAttempts configures the maximum number of attempts the publisher will make
// to send a message and wait for a confirm. Set to 1 for at-most-once delivery (no retries).
// Default is 3 for at-least-once delivery.
func (p *Publisher) SetMaxAttempts(attempts int) {
	p.lock.Lock()
	defer p.lock.Unlock()
	if attempts < 1 {
		attempts = 1
	}
	p.maxAttempts = attempts
}

func (p *Publisher) startConfirmListener(ch *amqp.Channel) {
	confirmChan := ch.NotifyPublish(make(chan amqp.Confirmation, 1000))

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for conf := range confirmChan {
			if chObj, ok := p.pendingConfirms.LoadAndDelete(conf.DeliveryTag); ok {
				resultChan := chObj.(chan error)

				var err error
				if !conf.Ack {
					err = errors.New("message not acknowledged by broker")
				}

				select {
				case resultChan <- err:
				default:
				}
			}
		}
	}()
}

func (p *Publisher) reconnectLoop() {

	backoff := time.Second

	for {

		select {
		case <-p.done:
			return
		default:
		}

		p.lock.RLock()
		ch := p.ch
		p.lock.RUnlock()

		if ch == nil {

			select {
			case <-p.done:
				return
			case <-time.After(backoff):
			}

			continue
		}

		closeChan := ch.NotifyClose(
			make(chan *amqp.Error, 1),
		)

		err := <-closeChan

		select {
		case <-p.done:
			return
		default:
		}

		// channel already replaced
		p.lock.RLock()
		currentCh := p.ch
		p.lock.RUnlock()

		if currentCh != ch {
			continue
		}

		if err != nil {
			p.client.log.Warn().
				Err(err).
				Msg("RabbitMQ publisher channel lost")
		} else {
			p.client.log.Warn().
				Msg("RabbitMQ publisher channel closed unexpectedly")
		}

		for {

			select {
			case <-p.done:
				return
			default:
			}

			newCh, e := p.client.NewChannel()
			if e != nil {

				p.client.log.Error().
					Err(e).
					Msg("Failed to recreate publisher channel")

				select {
				case <-p.done:
					return
				case <-time.After(backoff):
				}

				if backoff < 30*time.Second {
					backoff *= 2
				}

				continue
			}

			e = newCh.Confirm(false)
			if e != nil {

				_ = newCh.Close()

				p.client.log.Error().
					Err(e).
					Msg("Failed to enable publisher confirm mode")

				select {
				case <-p.done:
					return
				case <-time.After(backoff):
				}

				if backoff < 30*time.Second {
					backoff *= 2
				}

				continue
			}

			p.lock.Lock()

			select {
			case <-p.done:
				p.lock.Unlock()
				_ = newCh.Close()
				return
			default:
			}

			oldCh := p.ch

			// Reject all pending confirms with error because channel is recreated
			p.pendingConfirms.Range(func(key, value any) bool {
				resultChan := value.(chan error)
				select {
				case resultChan <- errors.New("publisher channel reconnected"):
				default:
				}
				p.pendingConfirms.Delete(key)
				return true
			})

			p.ch = newCh

			p.startConfirmListener(newCh)

			p.lock.Unlock()

			if oldCh != nil && !oldCh.IsClosed() {
				_ = oldCh.Close()
			}

			backoff = time.Second

			p.client.log.Info().
				Msg("RabbitMQ publisher channel recreated")

			break
		}
	}
}

func (p *Publisher) channel() (*amqp.Channel, error) {

	p.lock.RLock()

	ch := p.ch

	p.lock.RUnlock()

	if ch == nil || ch.IsClosed() {
		return nil, errors.New(
			"publisher channel unavailable",
		)
	}

	return ch, nil
}

func (p *Publisher) Publish(
	ctx context.Context,
	exchange string,
	routingKey string,
	msg amqp.Publishing,
	mandatory bool,
	immediate bool,
) error {

	var lastErr error

	if msg.MessageId == "" {
		msg.MessageId = uuid.NewString()
	}
	if msg.Timestamp.IsZero() {
		msg.Timestamp = time.Now().UTC()
	}

	p.lock.RLock()
	attempts := p.maxAttempts
	p.lock.RUnlock()

	for i := 0; i < attempts; i++ {
		err := p.publishOnce(ctx, exchange, routingKey, msg, mandatory, immediate)
		if err == nil {
			return nil
		}

		lastErr = err

		if i == attempts-1 {
			break
		}

		select {
		case <-ctx.Done():
			return errors.Join(lastErr, ctx.Err())
		case <-p.done:
			return errors.Join(lastErr, errors.New("publisher closed"))
		case <-time.After(time.Second):
		}
	}

	if lastErr == nil {
		lastErr = errors.New("failed to publish message after max attempts")
	}
	return lastErr
}

func (p *Publisher) publishOnce(
	ctx context.Context,
	exchange string,
	routingKey string,
	msg amqp.Publishing,
	mandatory bool,
	immediate bool,
) error {
	publishCtx, cancel := context.WithTimeout(ctx, p.client.publishTimeout())
	defer cancel()

	p.publishLock.Lock()

	ch, err := p.channel()
	if err != nil {
		p.publishLock.Unlock()
		return err
	}

	seqNo := ch.GetNextPublishSeqNo()
	resultChan := make(chan error, 1)
	p.pendingConfirms.Store(seqNo, resultChan)

	err = ch.PublishWithContext(
		publishCtx,
		exchange,
		routingKey,
		mandatory,
		immediate,
		msg,
	)

	if err != nil {
		p.pendingConfirms.Delete(seqNo)
		p.publishLock.Unlock()
		return err
	}

	p.publishLock.Unlock()

	select {
	case resErr := <-resultChan:
		return resErr

	case <-p.done:
		p.pendingConfirms.Delete(seqNo)
		return errors.New("publisher closed while waiting for confirm")

	case <-publishCtx.Done():
		p.pendingConfirms.Delete(seqNo)
		return errors.Join(errors.New("publisher publish timeout"), publishCtx.Err())
	}
}

func (p *Publisher) Close() error {

	p.lock.Lock()

	select {
	case <-p.done:
		p.lock.Unlock()
		return nil
	default:
		close(p.done)
	}

	ch := p.ch
	p.ch = nil

	p.lock.Unlock()

	if ch != nil && !ch.IsClosed() {
		_ = ch.Close()
	}

	p.wg.Wait()

	return nil
}
