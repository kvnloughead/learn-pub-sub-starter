package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	rbtConnStr := "amqp://guest:guest@localhost:5672/"

	rbtConn, err := amqp.Dial(rbtConnStr)
	if err != nil {
		log.Fatal("Failed to connect to RabbitMQ")
	}
	defer rbtConn.Close()

	fmt.Println("Connected to RabbitMQ on port 5672")
	fmt.Println("Starting Peril server...")

	// Shutdown on Ctrl+C
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan

	fmt.Println("\nShutting down Peril server")
}
