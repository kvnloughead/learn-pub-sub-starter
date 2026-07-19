package pubsub

import (
	"context"
	"encoding/json"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
	DurableQueue SimpleQueueType = iota
	TransientQueue
)

var SimpleQueueTypeMap = map[SimpleQueueType]string{
	DurableQueue:   "durable",
	TransientQueue: "transient",
}

func (sqt SimpleQueueType) String() string {
	return SimpleQueueTypeMap[sqt]
}

// DeclareAndBind opens a new channel on the received connection, declares a
// queue with the given name and queue type, and binds it to the exhange,
// returning the channel and the queue.
//
// If queueType is durable, autodelete and exclusive are false. If queueType is
// transient, autodelete and exclusive are true. noWait is always false.
func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {

	rabbitChan, err := conn.Channel()
	if err != nil {
		log.Fatal("Failed to create rabbit channel")
	}

	isDurable := queueType == DurableQueue
	isTransient := queueType == TransientQueue

	queue, err := rabbitChan.QueueDeclare(queueName, isDurable, isTransient, isTransient, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	err = rabbitChan.QueueBind(queueName, key, exchange, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	return rabbitChan, queue, nil
}

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	jsonBytes, err := json.Marshal(val)
	if err != nil {
		return err
	}

	err = ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{ContentType: "application/json", Body: jsonBytes})
	if err != nil {
		return err
	}

	return nil
}
