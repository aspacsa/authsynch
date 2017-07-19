package client

import (
	"authsynch/producer/logtypes"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

/*
	Start to be able to connect to kafka as producer.
	Call  to initiate client.
*/
func Send(brokers string, topic string, message string) {
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

	doneChannel := make(chan bool)

	go func() {
	outer:
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				m := ev
				if m.TopicPartition.Error != nil {
					logtypes.Error.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
				} else {
					logtypes.Info.Printf("Delivered message to topic %s [%d] at offset %v\n",
						*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
				}
				break outer
			default:
				logtypes.Info.Printf("Ignored event: %s\n", ev)
			}
		}
		close(doneChannel)
	}()

	producer.ProduceChannel() <- &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny}, Value: []byte(message)}

	// wait for delivery report goroutine to finish
	_ = <-doneChannel

	producer.Close()

}
