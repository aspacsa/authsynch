package client

import (
	"authsynch/producer/logtypes"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

//Server is the structure used to configure server setting.
type Server struct {
	Brokers string
	Topic   string
}

//Send message to the broker.
func Send(server *Server, message string) error {
	producer := configure((*server).Brokers)

	doneChannel := make(chan bool)

	go func() {
	outer:
		for event := range producer.Events() {
			switch ev := event.(type) {
			case *kafka.Message:
				message := ev
				if message.TopicPartition.Error != nil {
					logtypes.Error.Printf("Delivery failed: %v\n", message.TopicPartition.Error)
				} else {
					logtypes.Info.Printf("Delivered message to topic %s [%d] at offset %v\n",
						*message.TopicPartition.Topic, message.TopicPartition.Partition, message.TopicPartition.Offset)
				}
				break outer
			default:
				logtypes.Info.Printf("Ignored event: %s\n", ev)
			}
		}
		close(doneChannel)
	}()

	producer.ProduceChannel() <- &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &server.Topic, Partition: kafka.PartitionAny},
		Value:          []byte(message)}
	// wait for delivery report goroutine to finish
	_ = <-doneChannel
	producer.Close()

	return nil
}

// Configure the producer client.
func configure(brokers string) *kafka.Producer {
	host, _ := os.Hostname()
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":    brokers,
		"client.id":            host,
		"default.topic.config": kafka.ConfigMap{"acks": "all"}})

	if err != nil {
		logtypes.Error.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}
	logtypes.Info.Printf("Created producer %v\n", producer)
	return producer
}
