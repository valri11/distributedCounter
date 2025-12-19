package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"github.com/valri11/distributedcounter/types"
)

type kafkaPublisher struct {
	url   string
	topic string

	producer  *kafka.Producer
	messageId atomic.Int64
}

func NewKafkaPublisher(url string, params map[string]string) (*kafkaPublisher, error) {
	topic := params["topic"]

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": url})
	if err != nil {
		return nil, err
	}

	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					slog.Error("delivery failed", "partition", ev.TopicPartition)
				} else {
					slog.Info("delivered message", "partition", ev.TopicPartition)
				}
			}
		}
	}()

	kp := kafkaPublisher{
		url:      url,
		topic:    topic,
		producer: p,
	}

	err = kp.initializeKafkaTopic(url, topic)
	if err != nil {
		return nil, err
	}

	return &kp, nil
}

func (p *kafkaPublisher) ReportUsage(ctx context.Context, resUsage []types.AccountUsage) error {
	var msg []*kafka.Message
	for _, ru := range resUsage {
		msgID := p.messageId.Add(1)
		ru.MessageID = msgID
		jsonData, err := json.Marshal(ru)
		if err != nil {
			return err
		}
		msg = append(msg, &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &p.topic,
				Partition: kafka.PartitionAny,
			},
			Key:   []byte(ru.AccountID),
			Value: []byte(jsonData),
		})
	}

	for _, m := range msg {
		err := p.producer.Produce(m, nil)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *kafkaPublisher) initializeKafkaTopic(brokers, topicName string) error {
	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
	})
	if err != nil {
		return err
	}
	defer adminClient.Close()

	log.Printf("Creating topic '%s'...", topicName)
	topicSpec := kafka.TopicSpecification{
		Topic:             topicName,
		NumPartitions:     10,
		ReplicationFactor: 1,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	results, err := adminClient.CreateTopics(ctx, []kafka.TopicSpecification{topicSpec})
	if err != nil {
		return err
	}
	for _, result := range results {
		if result.Error.Code() == kafka.ErrTopicAlreadyExists {
			slog.Info("topic already exists", "error", result.Error)
			continue
		}
		if result.Error.Code() != kafka.ErrNoError {
			return fmt.Errorf("failed to create topic: %v", result.Error)
		}
		slog.Info("topic created successfully", "topic", result.Topic)
	}

	return c.waitForTopicReady(brokers, topicName)
}

func (c *kafkaPublisher) waitForTopicReady(brokers, topicName string) error {
	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
	})
	if err != nil {
		return err
	}
	defer adminClient.Close()

	for {
		time.Sleep(1 * time.Second)
		metadata, err := adminClient.GetMetadata(&topicName, false, 5000)

		if err != nil {
			slog.Error("metadata fetch failed", "error", err)
			continue
		}

		topicMeta, exists := metadata.Topics[topicName]
		if !exists {
			continue
		}
		if len(topicMeta.Partitions) > 0 {
			allPartitionsReady := true
			for _, partition := range topicMeta.Partitions {
				if partition.Error.Code() != kafka.ErrNoError {
					allPartitionsReady = false
					break
				}
				if partition.Leader == -1 {
					allPartitionsReady = false
					break
				}
			}

			slog.Info("topic ready")

			if allPartitionsReady {
				return nil
			}
		}
	}
}
