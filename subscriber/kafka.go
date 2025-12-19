package subscriber

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
)

type kafkaMessageProvider struct {
	brokerUrl     string
	topic         string
	consumerGroup string
	handler       MessageHandler

	reader *kafka.Reader
}

func NewKafkaMessageProvider(ctx context.Context,
	brokerUrl string,
	options map[string]string,
	handler MessageHandler,
) (*kafkaMessageProvider, error) {
	topic := options["topic"]
	consumerGroup := options["consumer-group"]

	mp := kafkaMessageProvider{
		brokerUrl:     brokerUrl,
		topic:         topic,
		consumerGroup: consumerGroup,
		handler:       handler,
	}

	return &mp, nil
}

func (p *kafkaMessageProvider) Close() error {
	if p.reader != nil {
		p.reader.Close()
	}
	return nil
}

func (p *kafkaMessageProvider) Status() map[string]bool {
	return nil
}

func (p *kafkaMessageProvider) Subscribe(ctx context.Context, _ MessageSubscriberConfig) error {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{p.brokerUrl},
		GroupID:  p.consumerGroup,
		Topic:    p.topic,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
		MaxWait:  5 * time.Second,
	})

	p.reader = reader

	go func() {
		log.Println("Starting consumer group...")
	LOOP_OUT:
		for {
			m, err := reader.FetchMessage(context.Background())
			if err != nil {
				log.Printf("Error fetching message: %v", err)
				break
			}
			// Process the message
			fmt.Printf("Received: Topic=%s, Partition=%d, Offset=%d, Value=%s\n",
				m.Topic, m.Partition, m.Offset, string(m.Value))

			msg := Message{
				Body: m.Value,
			}
			action := p.handler(ctx, msg)

			switch action {
			case Ack:
				// Commit the message offset (important for group tracking)
				if err := reader.CommitMessages(context.Background(), m); err != nil {
					log.Printf("Failed to commit message: %v", err)
				}
			default:
			}
			if err != nil {
				slog.Error("error consume kafka message", "error", err)
				break LOOP_OUT
			}
		}
	}()

	return nil
}

func (p *kafkaMessageProvider) Unsubscribe(context.Context, string) error {
	return p.Close()
}
