package subscriber

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
)

type MessageAction int

const (
	// NAckRequeue returns the message back to the broker
	NAckRequeue MessageAction = iota
	// NAckReject tells the broker that the message is invalid (to be dropped or put in DLQ)
	NAckReject
	// Ack notifies about message processing success
	Ack
	// NoAction indicates that the message handler performed Ack/NAck actions
	NoAction
)

type Message struct {
	ID   string
	Body []byte
}

type MessageHandler func(context.Context, Message) MessageAction

type Processor interface {
	MessageHandler(ctx context.Context, msg Message) MessageAction
}

type Channel interface {
	Close() error
	ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
}

type Connection interface {
	Close() error
	Channel() (Channel, error)
	NotifyClose(receiver chan *amqp.Error) chan *amqp.Error
}

type ConnectionWrapper struct {
	Conn *amqp.Connection
}

func (w ConnectionWrapper) Close() error {
	return w.Conn.Close()
}

func (w ConnectionWrapper) Channel() (Channel, error) {
	return w.Conn.Channel()
}

func (w ConnectionWrapper) NotifyClose(receiver chan *amqp.Error) chan *amqp.Error {
	return w.Conn.NotifyClose(receiver)
}

type MessageSubscriberConfig interface {
	BindAndConsume(context.Context, Connection) (Channel, error)
}

type MessageProvider interface {
	Close() error

	Subscribe(context.Context, MessageSubscriberConfig) error
}
