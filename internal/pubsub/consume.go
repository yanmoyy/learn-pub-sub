package pubsub

import (
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
	SimpleQueueDurable SimpleQueueType = iota
	SimpleQueueTransient
)

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	simpleQueueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("could not create channel: %v", err)
	}
	queue, err := ch.QueueDeclare(
		queueName,                             // name
		simpleQueueType == SimpleQueueDurable, // durable
		simpleQueueType != SimpleQueueDurable, // delete when unused
		simpleQueueType != SimpleQueueDurable, // exclusive
		false,                                 // no-wait
		nil,                                   // arguments
	)
	if err != nil {
		return nil, amqp.Queue{}, err
	}
	err = ch.QueueBind(
		queue.Name, // queue name
		key,        // routing key
		exchange,   // exchange
		false,      // no-wait
		nil,        // args
	)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("could not bind queue: %v", err)
	}
	return ch, queue, nil
}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	simgSimpleQueueType SimpleQueueType,
	handler func(T),
) error {
	ch, queue, err := DeclareAndBind(
		conn, exchange, queueName, key, simgSimpleQueueType,
	)
	if err != nil {
		return fmt.Errorf("could not declare and bind queue: %v", err)
	}
	deliveryCh, err := ch.Consume(
		queue.Name, // queue
		"",         // consumer (auto-generated)
		false,      // autoAck
		false,      // exclusive
		false,      // noLocal
		false,      // noWait
		nil,        // args
	)
	if err != nil {
		return fmt.Errorf("could not consume message: %v", err)
	}
	go func() {
		defer ch.Close()
		for delivery := range deliveryCh {
			var msg T
			err := json.Unmarshal(delivery.Body, &msg)
			if err != nil {
				fmt.Printf("could not unmarshal message: %v\n", err)
				return
			}
			handler(msg)
			err = delivery.Ack(false)
			if err != nil {
				fmt.Printf("could not Ack: %v\n", err)
			}
		}
	}()
	return nil
}
