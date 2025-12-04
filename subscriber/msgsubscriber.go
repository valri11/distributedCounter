package subscriber

import (
	"context"
	"fmt"

	"github.com/valri11/distributedcounter/telemetry"
	"go.opentelemetry.io/otel"
)

type MsgSubscriberConfig struct {
	exchangeName string
	queueName    string
	headers      map[string]any
	handler      MessageHandler
}

func NewMessageSubscriberConfig(
	exchangeName string,
	queueName string,
	headers map[string]any,
	handler MessageHandler,
) *MsgSubscriberConfig {
	cfg := MsgSubscriberConfig{
		exchangeName: exchangeName,
		queueName:    queueName,
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

	exchangeType := "headers"

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

	q, err := ch.QueueDeclare(
		msc.queueName, // name
		true,          // durable
		false,         // delete when unused
		false,         // exclusive
		false,         // no-wait
		nil,           // arguments

	)
	if err != nil {
		return nil, fmt.Errorf("declare AMQP queue: %w", err)
	}

	err = ch.QueueBind(
		q.Name,           // queue name
		"",               // routing key
		msc.exchangeName, // exchange
		false,
		msc.headers)
	if err != nil {
		return nil, fmt.Errorf("bind AMQP queue: %w", err)
	}

	go func(ctx context.Context) {
		msgs, err := ch.Consume(
			msc.queueName, // queue
			"",            // consumer
			false,         // auto-ack
			false,         // exclusive
			false,         // no-local
			false,         // no-wait
			nil,           // args
		)
		if err != nil {
			//log.WithError(err).Error("consume AMQP messages")
			return
		}

		tracer, ok := telemetry.TracerFromContext(ctx)
		if !ok {
			tracer = otel.Tracer("eventmanager")
		}

		//tag := fmt.Sprintf("queue:%s", msc.queueName)

		for msg := range msgs {
			ctx, span := tracer.Start(ctx, "receiveMessage")
			//stats.IncCount(ctx, "eventmanager.msg_recv", tag)

			/*
				log.WithField("msgId", msg.MessageId).
					WithField("exch", msg.Exchange).
					WithField("headers", msg.Headers).
					Debug("received a message")
			*/

			action := msc.handler(ctx, Message{
				ID:   msg.MessageId,
				Body: msg.Body,
			})
			var err error
			switch action {
			case Ack:
				err = msg.Ack(false)
				//stats.IncCount(ctx, "eventmanager.msg_ack", tag)
			case NAckReject:
				err = msg.Reject(false)
				//stats.IncCount(ctx, "eventmanager.msg_reject", tag)
			case NAckRequeue:
				err = msg.Reject(true)
				//stats.IncCount(ctx, "eventmanager.msg_requeue", tag)
			case NoAction:
				//stats.IncCount(ctx, "eventmanager.msg_noaction", tag)
			}
			if err != nil {
				//log.WithError(err).Error("Consume AMQP message", tag)
			}

			span.End()
		}
	}(ctx)

	return ch, nil
}
