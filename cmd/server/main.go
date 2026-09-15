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
	rbtConn, rbtChan, err := pubsub.ConnectToRabbit(rbtConnStr)
	if err != nil {
		log.Fatal("Failed to connect to RabbitMQ")
	}
	defer rbtConn.Close()

	fmt.Println("Connected to RabbitMQ on port 5672")
	fmt.Println("Starting Peril server...")
	gamelogic.PrintServerHelp()

	// game_logs
	err = pubsub.SubscribeGob(rbtConn, routing.ExchangePerilTopic, routing.GameLogSlug, routing.GameLogSlug+".*", pubsub.Durable, handlerGameLogs())
	if err != nil {
		log.Fatal("Failed to subscribe to the game_logs queue")
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	inputChan := make(chan []string)
	readyChan := make(chan struct{})
	go func() {
		for {
			<-readyChan // block
			inputChan <- gamelogic.GetInput()
		}
	}()
	readyChan <- struct{}{} // unblock

loop:
	for {
		select {
		case <-signalChan:
			fmt.Println("\nShutting down Peril server")
			break loop

		case words := <-inputChan:
			if len(words) == 0 {
				readyChan <- struct{}{}
				continue
			}

			cmd := words[0]
			switch cmd {
			case "pause":
				err = pubsub.PublishJSON(rbtChan, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true})
				if err != nil {
					log.Fatal("Failed to publish pause command")
				}
				fmt.Println("Sending pause message")

			case "resume":
				err = pubsub.PublishJSON(rbtChan, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: false})
				if err != nil {
					log.Fatal("Failed to publish unpause command")
				}
				fmt.Println("Sending unpause message")

			case "quit":
				fmt.Println("Exiting program")
				break loop

			default:
				fmt.Println("Unknown command")
			}

			readyChan <- struct{}{} // unblock
		}
	}
}

func handlerGameLogs() func(gl routing.GameLog) pubsub.AckType {
	return func(gl routing.GameLog) pubsub.AckType {
		defer fmt.Print("> ")

		err := gamelogic.WriteLog(gl)
		if err != nil {
			return pubsub.NackRequeue
		}

		return pubsub.Ack
	}

}
