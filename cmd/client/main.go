package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	rbtConnStr := "amqp://guest:guest@localhost:5672/"
	rbtConn, rbtChan, err := pubsub.ConnectToRabbit(rbtConnStr)
	if err != nil {
		log.Fatal("Failed to connect to RabbitMQ")
	}
	defer rbtConn.Close()

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatal(err.Error())
	}

	gs := gamelogic.NewGameState(username)

	pauseQueueName := fmt.Sprintf("%s.%s", routing.PauseKey, username)
	err = pubsub.SubscribeJSON(rbtConn, routing.ExchangePerilDirect, pauseQueueName, routing.PauseKey, pubsub.Transient, handlerPause(gs))
	if err != nil {
		log.Fatal("Failed to declare and bind to pause queue")
	}

	armyMovesQueueName := fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, username)
	err = pubsub.SubscribeJSON(rbtConn, routing.ExchangePerilTopic, armyMovesQueueName, routing.ArmyMovesKey, pubsub.Transient, handlerMove(gs, rbtChan))
	if err != nil {
		log.Fatal("Failed to declare and bind to army moves queue: " + err.Error())
	}

	warQueueKey := routing.WarRecognitionsPrefix + ".*"
	err = pubsub.SubscribeJSON(rbtConn, routing.ExchangePerilTopic, "war", warQueueKey, pubsub.Durable, handlerWar(gs, rbtChan))
	if err != nil {
		log.Fatal("Failed to declare and bind to war queue: " + err.Error())
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
				move, err := gs.CommandMove(words)
				if err != nil {
					fmt.Println(err.Error())
				}

				err = pubsub.PublishJSON(rbtChan, routing.ExchangePerilTopic, armyMovesQueueName, move)
				if err != nil {
					log.Fatal("Failed to publish move")
				}
				fmt.Println("Sending move message")

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

func handlerPause(gs *gamelogic.GameState) func(ps routing.PlayingState) pubsub.AckType {
	return func(ps routing.PlayingState) pubsub.AckType {
		defer fmt.Println(("> "))
		gs.HandlePause(ps)
		return pubsub.Ack
	}
}

func handlerMove(gs *gamelogic.GameState, rbtChan *amqp.Channel) func(move gamelogic.ArmyMove) pubsub.AckType {
	return func(move gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Println(("> "))
		outcome := gs.HandleMove(move, rbtChan)

		if outcome == gamelogic.MoveOutcomeSafe || outcome == gamelogic.MoveOutcomeMakeWar {
			return pubsub.Ack
		}
		if outcome == gamelogic.MoveOutcomePublishingFailed {
			return pubsub.NackRequeue
		}
		if outcome == gamelogic.MoveOutcomeSamePlayer {
			return pubsub.NackDiscard
		}
		return pubsub.NackDiscard
	}
}

func handlerWar(gs *gamelogic.GameState, rbtChan *amqp.Channel) func(rw gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(rw gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")
		outcome, winner, loser := gs.HandleWar(rw)

		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackRequeue

		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard

		case gamelogic.WarOutcomeOpponentWon:
			msg := fmt.Sprintf("%s won a war against %s", winner, loser)
			err := publishGameLog(gs, rbtChan, msg)
			if err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack

		case gamelogic.WarOutcomeYouWon:
			msg := fmt.Sprintf("%s won a war against %s", winner, loser)
			err := publishGameLog(gs, rbtChan, msg)
			if err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack

		case gamelogic.WarOutcomeDraw:
			msg := fmt.Sprintf("A war between %s and %s resulted in a draw", winner, loser)
			err := publishGameLog(gs, rbtChan, msg)
			if err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack

		default:
			return pubsub.NackDiscard
		}
	}
}

func publishGameLog(gs *gamelogic.GameState, rbtChan *amqp.Channel, msg string) error {
	key := routing.GameLogSlug + "." + gs.Player.Username
	v := routing.GameLog{
		CurrentTime: time.Now(),
		Message:     msg,
		Username:    gs.Player.Username,
	}

	err := pubsub.PublishGob(rbtChan, routing.ExchangePerilTopic, key, &v)
	if err != nil {
		return err
	}

	return nil
}
