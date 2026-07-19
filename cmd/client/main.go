package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	rbtURL := "amqp://guest:guest@localhost:5672/"
	rbtConn, err := amqp.Dial(rbtURL)
	if err != nil {
		log.Fatal("Failed to connect to RabbitMQ")
	}
	defer rbtConn.Close()
	fmt.Println("RabbitMQ connection successful")

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatal("Failed to receive username")
	}

	pauseQueueName := fmt.Sprintf("%s.%s", routing.PauseKey, username)

	_, _, err = pubsub.DeclareAndBind(rbtConn, routing.ExchangePerilDirect, pauseQueueName, routing.PauseKey, pubsub.TransientQueue)
	if err != nil {
		log.Fatal("Failed to declare and bind the pause queue")
	}

	fmt.Println("Starting Peril client...")

	// Block until Ctrl+C is received
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan

	fmt.Println("RabbitMQ connection closed")
}
