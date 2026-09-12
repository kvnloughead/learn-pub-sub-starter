package pubsub

import (
	"context"
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

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	jsonVal, err := json.Marshal(val)
	if err != nil {
		return err
	}

	msg := amqp.Publishing{
		ContentType: "application/json",
		Body:        jsonVal,
	}
	ch.PublishWithContext(context.Background(), exchange, key, false, false, msg)

	return nil
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
	queue, err := rbtChan.QueueDeclare(queueName, isDurable, !isDurable, !isDurable, false, nil)
	if err != nil {
		fmt.Println("error", err.Error())
		return nil, amqp.Queue{}, err
	}

	err = rbtChan.QueueBind(queueName, key, routing.ExchangePerilDirect, false, nil)
	if err != nil {
		fmt.Println("error", err.Error())
		return nil, amqp.Queue{}, err
	}

	return rbtChan, queue, err
}
