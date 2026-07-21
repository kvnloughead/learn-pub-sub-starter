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
	gameState := gamelogic.NewGameState(username)

	startClientCommandLoop(gameState)
}

// startClientCommandLoop starts a loop that waits for user commands. It runs
// until the user enters 'quit' or Ctrl+C. Type 'help' when the server is
// running to see the possible commands.
func startClientCommandLoop(gameState *gamelogic.GameState) {
	exitLoop := false
	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}

		switch w := words[0]; w {
		case "spawn":
			gameState.CommandSpawn(words)

		case "move":
			_, err := gameState.CommandMove(words)
			if err != nil {
				fmt.Println("Invalid move")
				continue
			}

		case "status":
			gameState.CommandStatus()

		case "help":
			gamelogic.PrintClientHelp()

		case "spam":
			fmt.Println("Spamming not allowed yet!")

		case "quit":
			gamelogic.PrintQuit()
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
