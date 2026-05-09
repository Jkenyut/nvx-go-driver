package rabbitmq

import (
	"context"
	"errors"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Publisher struct {
	client *Client

	lock sync.RWMutex

	ch *amqp.Channel

	publishLock     sync.Mutex
	pendingConfirms sync.Map

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
		client: client,
		ch:     ch,
		done:   make(chan struct{}),
	}

	p.startConfirmListener(ch)

	p.wg.Add(1)

	go func() {
		defer p.wg.Done()
		p.reconnectLoop()
	}()

	return p, nil
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

		err, ok := <-closeChan

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

		// graceful close
		if !ok || err == nil {

			select {
			case <-p.done:
				return
			case <-time.After(backoff):
			}

			continue
		}

		p.client.log.Warn().
			Err(err).
			Msg("RabbitMQ publisher channel lost")

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

			p.lock.Unlock()

			p.startConfirmListener(newCh)

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

	for i := 0; i < 3; i++ {

		p.publishLock.Lock()

		ch, err := p.channel()
		if err != nil {
			p.publishLock.Unlock()
			lastErr = err
			time.Sleep(time.Second)
			continue
		}

		seqNo := ch.GetNextPublishSeqNo()
		resultChan := make(chan error, 1)
		p.pendingConfirms.Store(seqNo, resultChan)

		err = ch.PublishWithContext(
			ctx,
			exchange,
			routingKey,
			mandatory,
			immediate,
			msg,
		)

		if err != nil {
			p.pendingConfirms.Delete(seqNo)
			p.publishLock.Unlock()
			lastErr = err
			time.Sleep(time.Second)
			continue
		}

		p.publishLock.Unlock()

		select {

		case resErr := <-resultChan:

			if resErr != nil {
				lastErr = resErr
				time.Sleep(time.Second)
				continue
			}

			return nil

		case <-ctx.Done():
			p.pendingConfirms.Delete(seqNo)
			return ctx.Err()

		case <-time.After(5 * time.Second):
			p.pendingConfirms.Delete(seqNo)
			lastErr = errors.New("publisher confirm timeout")
			time.Sleep(time.Second)
		}
	}

	return lastErr
}

func (p *Publisher) Close() error {

	select {
	case <-p.done:
	default:
		close(p.done)
	}

	p.lock.Lock()

	ch := p.ch
	p.ch = nil

	p.lock.Unlock()

	if ch != nil && !ch.IsClosed() {
		_ = ch.Close()
	}

	p.wg.Wait()

	return nil
}
