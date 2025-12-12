package queuemanager

import "context"

type AMQPClient interface {
	GetExchangeSourceQueues(ctx context.Context, vhost string, exchangeName string) ([]string, error)
}

type QueueManager struct {
	VHost        string
	ExchangeName string
	amqpClient   AMQPClient
}

func NewQueueManager(amqpClient AMQPClient, vhost string, exchangeName string) (*QueueManager, error) {
	qm := QueueManager{
		VHost:        vhost,
		ExchangeName: exchangeName,
		amqpClient:   amqpClient,
	}

	return &qm, nil
}

func (qm *QueueManager) GetExchangeQueues(ctx context.Context) ([]string, error) {
	return qm.amqpClient.GetExchangeSourceQueues(ctx, qm.VHost, qm.ExchangeName)
}
