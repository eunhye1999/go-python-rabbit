package pkg

import (
	"fmt"
	"log"

	"github.com/streadway/amqp"
)

// Constants for RabbitMQ configuration
const (
	rabbitMQURL = "amqp://guest:guest@localhost:5672/"
	queueName   = "send_message"
)

// MessageHandler is a function type for processing messages
type MessageHandler func(string)

// Worker represents a RabbitMQ message consumer
type Worker struct {
	conn          *amqp.Connection
	channel       *amqp.Channel
	queue         amqp.Queue
	handleMessage MessageHandler
}

// NewWorker creates and initializes a new Worker
func NewWorker(handler MessageHandler) (*Worker, error) {
	// Connect to RabbitMQ
	conn, err := amqp.Dial(rabbitMQURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to open a channel: %w", err)
	}

	// Declare queue
	q, err := ch.QueueDeclare(
		queueName,
		true,  // Durable
		false, // Auto delete
		false, // Exclusive
		false, // No-wait
		nil,   // Arguments
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to declare a queue: %w", err)
	}

	return &Worker{
		conn:          conn,
		channel:       ch,
		queue:         q,
		handleMessage: handler,
	}, nil
}

// Close properly cleans up worker resources
func (w *Worker) Close() {
	if w.channel != nil {
		w.channel.Close()
	}
	if w.conn != nil {
		w.conn.Close()
	}
}

// SetupQoS configures Quality of Service settings
func (w *Worker) SetupQoS() error {
	err := w.channel.Qos(
		1,     // Prefetch count
		0,     // Prefetch size
		false, // Global
	)
	if err != nil {
		return fmt.Errorf("failed to set QoS: %w", err)
	}
	return nil
}

// StartConsuming begins consuming messages from the queue
func (w *Worker) StartConsuming() error {
	// Set up QoS before consuming
	if err := w.SetupQoS(); err != nil {
		return err
	}

	// Consume messages
	msgs, err := w.channel.Consume(
		w.queue.Name,
		"",
		false, // Auto Ack (false to manually acknowledge)
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to register a consumer: %w", err)
	}

	// Process messages
	forever := make(chan bool)
	go func() {
		for msg := range msgs {

			// Process the message here
			w.processMessage(msg)

			msg.Ack(false) // Acknowledge message after processing
		}
	}()

	fmt.Println(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever

	return nil
}

// processMessage handles an incoming message
func (w *Worker) processMessage(msg amqp.Delivery) {
	// Call the handler function with the message body
	if w.handleMessage != nil {
		w.handleMessage(string(msg.Body))
	}
}

// FailOnError is a helper function for error handling
func FailOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

// GetQueueName returns the name of the queue
func (w *Worker) GetQueueName() string {
	return w.queue.Name
}
