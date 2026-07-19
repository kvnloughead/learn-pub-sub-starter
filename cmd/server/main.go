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

	rabbitChan, err := rbtConn.Channel()
	if err != nil {
		log.Fatal("Failed to create rabbit channel")
	}

	fmt.Println("Starting Peril server...")
	gamelogic.PrintServerHelp()

	exitLoop := false
	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}

		switch w := words[0]; w {
		case "pause":
			fmt.Println("Sending 'pause' message")
			err = pubsub.PublishJSON(rabbitChan, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true})
			if err != nil {
				log.Fatal("Failed to publish pause play state")
			}

		case "resume":
			fmt.Println("Sending 'resume' message")
			err = pubsub.PublishJSON(rabbitChan, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: false})
			if err != nil {
				log.Fatal("Failed to publish pause play state")
			}

		case "quit":
			fmt.Println("Exiting program")
			exitLoop = true

		default:
			fmt.Println("Unknown command")
		}

		if exitLoop {
			break
		}
	}

	// Block until Ctrl+C is received
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan

	fmt.Println("RabbitMQ connection closed")
}
