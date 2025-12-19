package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	fmt.Println("test kafka sub")

	brokerUrl := "localhost:9092"
	topic := "resourcecount"
	//partition := 3

	//readPartition(brokerUrl, topic, partition)

	topicConsumer(brokerUrl, topic)
}

func topicConsumer(brokerUrl string, topic string) {
	// Configuration for the consumer group
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{brokerUrl},
		GroupID:  "resourcecount-consumers", // Identifies the consumer group
		Topic:    topic,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
		MaxWait:  5 * time.Second,
	})
	defer reader.Close()

	log.Println("Starting consumer group...")
	for {
		m, err := reader.FetchMessage(context.Background())
		if err != nil {
			log.Printf("Error fetching message: %v", err)
			break
		}
		// Process the message
		fmt.Printf("Received: Topic=%s, Partition=%d, Offset=%d, Value=%s\n",
			m.Topic, m.Partition, m.Offset, string(m.Value))

		// Commit the message offset (important for group tracking)
		if err := reader.CommitMessages(context.Background(), m); err != nil {
			log.Printf("Failed to commit message: %v", err)
		}
	}
}

func readPartition(brokerUrl string, topic string, partition int) {
	conn, err := kafka.DialLeader(context.Background(), "tcp", brokerUrl, topic, partition)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}

	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	batch := conn.ReadBatch(10e3, 1e6) // fetch 10KB min, 1MB max

	b := make([]byte, 10e3) // 10KB max per message
	for {
		n, err := batch.Read(b)
		if err != nil {
			break
		}
		fmt.Println(string(b[:n]))
	}

	if err := batch.Close(); err != nil {
		log.Fatal("failed to close batch:", err)
	}

	if err := conn.Close(); err != nil {
		log.Fatal("failed to close connection:", err)
	}
}
