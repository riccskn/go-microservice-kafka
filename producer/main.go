package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
)

func main() {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
	})
	if err != nil {
		log.Fatalf("Error on create producer: %v", err)
	}
	defer producer.Close()

	topic := "example-topic"
	fmt.Println("Producer starting. Sending messages...")

	for i := 1; i <= 10; i++ {
		message := fmt.Sprintf("Message #%d", i)
		err := producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(message),
		}, nil)
		if err != nil {
			log.Printf("Error produce message: %v", err)
		} else {
			log.Printf("Message sent: %s", message)
		}
	}

	producer.Flush(5000)
	fmt.Println("All messages sent successfully")
}
