package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
)

func main() {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "example-group",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		log.Fatalf("Error on create consumer: %v", err)
	}
	defer consumer.Close()

	topic := "example-topic"
	err = consumer.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		log.Fatalf("Error to subscribe on topic: %v", err)
	}

	fmt.Println("C")

	log.Println("Consumer iniciado. Aguardando mensagens...")

	for {
		msg, err := consumer.ReadMessage(-1)
		if err != nil {
			log.Printf("Erro ao consumir mensagem: %v", err)
			continue
		}
		log.Printf("Mensagem recebida: %s", string(msg.Value))
	}
}
