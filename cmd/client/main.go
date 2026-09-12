package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

func main() {
	rbtConnStr := "amqp://guest:guest@localhost:5672/"
	rbtConn, _, err := pubsub.ConnectToRabbit(rbtConnStr)
	if err != nil {
		log.Fatal("Failed to connect to RabbitMQ")
	}
	defer rbtConn.Close()

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatal(err.Error())
	}

	pauseQueueName := fmt.Sprintf("%s.%s", routing.PauseKey, username)
	_, _, err = pubsub.DeclareAndBind(rbtConn, routing.ExchangePerilDirect, pauseQueueName, routing.PauseKey, pubsub.Durable)
	if err != nil {
		log.Fatal("Failed to declare and bind to pause queue")
	}

	// Shutdown on Ctrl+C
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan

	fmt.Println("\nShutting down Peril client")
}
