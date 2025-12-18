package publisher

import (
	"context"
	"encoding/json"

	"github.com/segmentio/kafka-go"
	"github.com/valri11/distributedcounter/types"
)

type kafkaPublisher struct {
	url   string
	topic string

	conn *kafka.Conn
}

func NewKafkaPublisher(url string, params map[string]string) (*kafkaPublisher, error) {
	partition := 0
	topic := params["topic"]

	conn, err := kafka.DialLeader(context.Background(), "tcp", url, topic, partition)
	if err != nil {
		return nil, err
	}

	//conn.SetWriteDeadline(time.Now().Add(30 * time.Second))

	kp := kafkaPublisher{
		url:   url,
		topic: topic,
		conn:  conn,
	}

	return &kp, nil
}

func (p *kafkaPublisher) ReportUsage(ctx context.Context, resUsage []types.AccountUsage) error {
	var msg []kafka.Message
	for _, ru := range resUsage {
		jsonData, err := json.Marshal(ru)
		if err != nil {
			return err
		}
		msg = append(msg, kafka.Message{
			Value: jsonData},
		)
	}

	_, err := p.conn.WriteMessages(msg...)

	return err
}
