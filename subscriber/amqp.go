package subscriber

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/gofrs/uuid/v5"
	amqp "github.com/rabbitmq/amqp091-go"
)

type NotifyClient struct {
	ID            uuid.UUID
	Notifications chan<- any
}

type amqpMessageProvider struct {
	brokerURL string
	vHost     string

	mx           sync.Mutex
	amqpClient   Connection
	amqpChannels []Channel
	subscribers  []MessageSubscriberConfig
}

func DefaultDialer(brokerUrl string, cfg amqp.Config) (Connection, error) {
	conn, err := amqp.DialConfig(brokerUrl, cfg)
	return ConnectionWrapper{Conn: conn}, err
}

func NewAMQPMessageProvider(
	ctx context.Context,
	amqpURL string,
	vHost string,
	amqpDialer func(string, amqp.Config) (Connection, error),
) (*amqpMessageProvider, error) {
	config := amqp.Config{
		Vhost:      vHost,
		Properties: amqp.NewConnectionProperties(),
	}

	conn, err := amqpDialer(amqpURL, config)
	if err != nil {
		return nil, fmt.Errorf("dial AMQP endpoint %s (vhost: %s): %w", amqpURL, vHost, err)
	}

	ew := amqpMessageProvider{
		brokerURL:    amqpURL,
		vHost:        vHost,
		amqpClient:   conn,
		amqpChannels: make([]Channel, 0),
	}

	go func(ctx context.Context) {
		// connection failure watcher
		for {
			reason, ok := <-ew.amqpClient.NotifyClose(make(chan *amqp.Error, 1))
			if !ok {
				log.Printf("AMQP connection closed")
				return
			}

			log.Printf("unexpected close of AMQP connection: %v", reason)

			retryInterval := 1 * time.Second
			backoffCoeff := 2
			maxRetryInterval := 30 * time.Second
			for {
				ew.mx.Lock()
				ew.amqpChannels = nil
				ew.mx.Unlock()

				if retryInterval < maxRetryInterval {
					retryInterval *= time.Duration(backoffCoeff)
					if retryInterval > maxRetryInterval {
						retryInterval = maxRetryInterval
					}
				}
				log.Printf("try reconnect to AMQP broker in %v", retryInterval)
				time.Sleep(retryInterval)

				config := amqp.Config{
					Vhost:      ew.vHost,
					Properties: amqp.NewConnectionProperties(),
				}

				conn, err := amqpDialer(ew.brokerURL, config)
				if err != nil {
					log.Printf("dial AMQP endpoint %s (vhost: %s): %v", ew.brokerURL, ew.vHost, err)
					continue
				}
				ew.mx.Lock()
				ew.amqpClient = conn
				ew.mx.Unlock()

				// recreate subscribers
				for _, subscr := range ew.subscribers {
					ch, err := subscr.BindAndConsume(ctx, ew.amqpClient)
					if err != nil {
						log.Printf("bind and consume: %v", err)
						continue
					}

					ew.mx.Lock()
					ew.amqpChannels = append(ew.amqpChannels, ch)
					ew.mx.Unlock()
				}

				log.Printf("successfully reconnected to AMQP broker")
				break
			}
		}
	}(ctx)

	return &ew, nil
}

func (ew *amqpMessageProvider) Subscribe(ctx context.Context, cfg MessageSubscriberConfig) error {
	// bind subscriber and start receive
	ch, err := cfg.BindAndConsume(ctx, ew.amqpClient)
	if err != nil {
		return fmt.Errorf("bind and consume: %w", err)
	}

	ew.mx.Lock()
	defer ew.mx.Unlock()

	ew.amqpChannels = append(ew.amqpChannels, ch)
	ew.subscribers = append(ew.subscribers, cfg)

	return nil
}

func (ew *amqpMessageProvider) Close() error {
	ew.mx.Lock()
	defer ew.mx.Unlock()

	for _, ch := range ew.amqpChannels {
		err := ch.Close()
		if err != nil {
			return fmt.Errorf("close AMQP channel: %w", err)
		}
	}
	ew.amqpChannels = nil

	if ew.amqpClient != nil {
		err := ew.amqpClient.Close()
		if err != nil {
			return fmt.Errorf("close AMQP connection: %w", err)
		}
		ew.amqpClient = nil
	}

	return nil
}
