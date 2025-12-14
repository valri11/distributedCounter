package subscriber

import (
	"context"
	"fmt"
	"log/slog"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/valri11/distributedcounter/telemetry"
	"go.opentelemetry.io/otel"
)

const (
	exchangeType = "x-consistent-hash"
)

type MsgSubscriberConfig struct {
	exchangeName string
	queueName    string
	consumerName string
	headers      map[string]any
	handler      MessageHandler
}

func NewMessageSubscriberConfig(
	exchangeName string,
	queueName string,
	consumerName string,
	headers map[string]any,
	handler MessageHandler,
) *MsgSubscriberConfig {
	cfg := MsgSubscriberConfig{
		exchangeName: exchangeName,
		queueName:    queueName,
		consumerName: consumerName,
		headers:      headers,
		handler:      handler,
	}
	return &cfg
}

func (c MsgSubscriberConfig) ExchangeName() string {
	return c.exchangeName
}

func (c MsgSubscriberConfig) QueueName() string {
	return c.queueName
}

func (c MsgSubscriberConfig) ConsumerName() string {
	return c.consumerName
}

func (c MsgSubscriberConfig) SubscriptionHeaders() map[string]any {
	return c.headers
}

func (c MsgSubscriberConfig) GetMessageHandler() MessageHandler {
	return c.handler
}

func (msc *MsgSubscriberConfig) BindAndConsume(ctx context.Context, conn Connection) (Channel, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("open AMQP channel: %w", err)
	}

	err = ch.ExchangeDeclare(
		msc.exchangeName, // name
		exchangeType,     // type
		true,             // durable
		false,            // auto-delete
		false,            // internal
		false,            // noWait
		nil,              // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("declare exchange: %v", err)
	}

	args := amqp.Table{
		"x-expires": int32(10 * 60000), // Queue expires after 10 min (unit is ms) of inactivity
	}

	q, err := ch.QueueDeclare(
		msc.queueName, // name
		true,          // durable
		false,         // delete when unused
		false,         // exclusive
		false,         // no-wait
		args,          // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("declare AMQP queue: %w", err)
	}

	err = ch.QueueBind(
		q.Name,           // queue name
		"1",              // routing key, for x-consistent-hash exchange this is a weight
		msc.exchangeName, // exchange
		false,
		msc.headers)
	if err != nil {
		return nil, fmt.Errorf("bind AMQP queue: %w", err)
	}

	go func(ctx context.Context) {
		consumerTag := fmt.Sprintf("cns-%s-%s", msc.consumerName, msc.queueName)
		msgs, err := ch.Consume(
			msc.queueName, // queue
			consumerTag,   // consumer tag
			false,         // auto-ack
			false,         // exclusive
			false,         // no-local
			false,         // no-wait
			nil,           // args
		)
		if err != nil {
			slog.Error("consume AMQP messages", "error", err)
			return
		}

		tracer, ok := telemetry.TracerFromContext(ctx)
		if !ok {
			tracer = otel.Tracer("eventmanager")
		}

	LOOP_OUT:
		for {
			select {
			case <-ctx.Done():
				break LOOP_OUT
			case msg, ok := <-msgs:
				if !ok {
					break LOOP_OUT
				}

				ctx, span := tracer.Start(ctx, "receiveMessage")
				action := msc.handler(ctx, Message{
					ID:   msg.MessageId,
					Body: msg.Body,
				})

				var err error
				switch action {
				case Ack:
					err = msg.Ack(false)
				case NAckReject:
					err = msg.Reject(false)
				case NAckRequeue:
					err = msg.Reject(true)
				case NoAction:
				}
				if err != nil {
					slog.Error("error consume AMQP message", "error", err)
					break LOOP_OUT
				}

				span.End()
			}
		}

		err = ch.Cancel(consumerTag, true)
		if err != nil {
			slog.Error("cancel consumer", "tag", consumerTag)
		}
	}(ctx)

	return ch, nil
}
