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

	gs := gamelogic.NewGameState(username)

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
			gamelogic.PrintQuit()
			break loop

		case words := <-inputChan:
			if len(words) == 0 {
				readyChan <- struct{}{}
				continue
			}

			cmd := words[0]
			switch cmd {
			case "spawn":
				err = gs.CommandSpawn(words)
				if err != nil {
					fmt.Println(err.Error())
				}

			case "move":
				_, err := gs.CommandMove(words)
				if err != nil {
					fmt.Println(err.Error())
				}

			case "status":
				gs.CommandStatus()

			case "help":
				gamelogic.PrintClientHelp()

			case "spam":
				fmt.Println("Spamming not allowed yet!")

			case "quit":
				gamelogic.PrintQuit()
				break loop

			default:
				fmt.Println("Unknown command")
			}

			readyChan <- struct{}{} // unblock
		}
	}
}
