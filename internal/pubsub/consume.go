package pubsub

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

type AckType int

type SimpleQueueType int

const (
	SimpleQueueDurable SimpleQueueType = iota
	SimpleQueueTransient
)

const (
	Ack AckType = iota
	NackDiscard
	NackRequeue
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
		amqp.Table{"x-dead-letter-exchange": routing.ExchangePerilDlx},
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
	handler func(T) AckType,
) error {
	return subscribe(conn,
		exchange,
		queueName,
		key,
		simgSimpleQueueType,
		handler,
		func(data []byte) (T, error) {
			var target T
			err := json.Unmarshal(data, &target)
			return target, err
		},
	)
}

func SubscribeGob[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	simgSimpleQueueType SimpleQueueType,
	handler func(T) AckType,
) error {
	return subscribe(conn,
		exchange,
		queueName,
		key,
		simgSimpleQueueType,
		handler,
		func(data []byte) (T, error) {
			buf := bytes.NewBuffer(data)
			decoder := gob.NewDecoder(buf)
			var target T
			err := decoder.Decode(&target)
			return target, err
		},
	)
}

func subscribe[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	simgSimpleQueueType SimpleQueueType,
	handler func(T) AckType,
	unmarshaller func(data []byte) (T, error),
) error {
	ch, queue, err := DeclareAndBind(
		conn, exchange, queueName, key, simgSimpleQueueType,
	)
	if err != nil {
		return fmt.Errorf("could not declare and bind queue: %v", err)
	}
	err = ch.Qos(10, 0, false)
	if err != nil {
		return fmt.Errorf("could not set Qos: %v", err)
	}
	msgCh, err := ch.Consume(
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
		for msg := range msgCh {
			target, err := unmarshaller(msg.Body)
			if err != nil {
				fmt.Printf("could not unmarshal message: %v\n", err)
				return
			}
			switch handler(target) {
			case Ack:
				_ = msg.Ack(false)
			case NackDiscard:
				_ = msg.Nack(false, false)
			case NackRequeue:
				_ = msg.Nack(false, true)
			}
		}
	}()
	return nil
}
