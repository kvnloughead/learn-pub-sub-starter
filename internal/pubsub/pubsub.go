package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType string

const (
	Durable   SimpleQueueType = "durable"
	Transient SimpleQueueType = "transient"
)

type AckType string

const (
	Ack         AckType = "ack"
	NackRequeue AckType = "nack_requeue"
	NackDiscard AckType = "nack_discard"
)

func ConnectToRabbit(connStr string) (*amqp.Connection, *amqp.Channel, error) {
	conn, err := amqp.Dial(connStr)
	if err != nil {
		return nil, nil, err
	}

	rbtChan, err := conn.Channel()
	if err != nil {
		return nil, nil, err
	}

	return conn, rbtChan, nil
}

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {
	rbtChan, err := conn.Channel()
	if err != nil {
		fmt.Println("error", err.Error())
		return nil, amqp.Queue{}, err
	}

	isDurable := queueType == Durable
	queue, err := rbtChan.QueueDeclare(queueName, isDurable, !isDurable, !isDurable, false, amqp.Table{"x-dead-letter-exchange": routing.ExchangePerilDeadLetter})
	if err != nil {
		fmt.Println("error", err.Error())
		return nil, amqp.Queue{}, err
	}

	err = rbtChan.QueueBind(queueName, key, exchange, false, nil)
	if err != nil {
		fmt.Println("error", err.Error())
		return nil, amqp.Queue{}, err
	}

	return rbtChan, queue, err
}

//
// encoding/json
//

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	jsonVal, err := json.Marshal(val)
	if err != nil {
		return err
	}

	msg := amqp.Publishing{
		ContentType: "application/json",
		Body:        jsonVal,
	}
	return ch.PublishWithContext(context.Background(), exchange, key, false, false, msg)
}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	unmarshalJSON := func(data []byte) (T, error) {
		var msg T
		err := json.Unmarshal(data, &msg)
		if err != nil {
			return msg, err
		}
		return msg, nil
	}

	return subscribe(conn, exchange, queueName, key, queueType, handler, unmarshalJSON)
}

//
// encoding/gob
//

func encode[T any](val T) ([]byte, error) {
	var data bytes.Buffer
	enc := gob.NewEncoder(&data)
	err := enc.Encode(val)
	if err != nil {
		return []byte{}, err
	}
	return data.Bytes(), nil
}

func decode[T any](data []byte) (T, error) {
	var b bytes.Buffer
	b.Write(data)

	var val T
	dec := gob.NewDecoder(&b)
	err := dec.Decode(&val)
	if err != nil {
		return val, err
	}

	return val, nil
}

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	data, err := encode(val)
	if err != nil {
		return err
	}

	msg := amqp.Publishing{
		ContentType: "application/gob",
		Body:        data,
	}
	return ch.PublishWithContext(context.Background(), exchange, key, false, false, msg)
}

func SubscribeGob[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	unmarshalGob := func(data []byte) (T, error) {
		msg, err := decode[T](data)
		if err != nil {
			return msg, err
		}
		return msg, nil
	}

	return subscribe(conn, exchange, queueName, key, queueType, handler, unmarshalGob)
}

func subscribe[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
	unmarshaller func([]byte) (T, error),
) error {
	rbtChan, _, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}

	deliveryChan, err := rbtChan.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	go func() {
		for delivery := range deliveryChan {
			msg, err := unmarshaller(delivery.Body)
			if err != nil {
				fmt.Println("error unmarshalling message:", err)
				continue
			}

			ack := handler(msg)
			switch ack {
			case Ack:
				delivery.Ack(false)
			case NackRequeue:
				delivery.Nack(false, true)
			case NackDiscard:
				delivery.Nack(false, false)
			}
		}
	}()

	return nil
}
