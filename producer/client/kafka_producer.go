package client

import (
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

/*
	Start to be able to connect to kafka as producer.
	Call  to initiate client.
*/
func Send(brokers []string, topic string, message string) {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":    brokers,
		"client.id":            os.Hostname,
		"default.topic.config": kafka.ConfigMap{"acks": "all"}})

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created producer %v\n", producer)

	doneChannel := make(chan bool)

	go func() {
	outer:
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				m := ev
				if m.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
				} else {
					fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
						*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
				}
				break outer
			default:
				fmt.Printf("Ignored event: %s\n", ev)
			}
		}
		close(doneChannel)
	}()

	value := "Hello Go!"
	producer.ProduceChannel() <- &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny}, Value: []byte(value)}

	// wait for delivery report goroutine to finish
	_ = <-doneChannel

	producer.Close()

}
