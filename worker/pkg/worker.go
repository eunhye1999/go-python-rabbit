package pkg

import (
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/streadway/amqp"
)

// Constants for RabbitMQ configuration
const (
	rabbitMQURL       = "amqp://guest:guest@localhost:5672/"
	queueName         = "send_message_queue"
	exchangeName      = "send_message_exchange"
	exchangeType      = "direct"
	delayExchangeName = "send_message_delay_exchange"
	retryQueueName    = "send_message_retry_queue"
	maxRetries        = 3
	retryDelay        = 1 * time.Minute // 1 minute retry delay
)

// MessageHandler is a function type for processing messages
type MessageHandler func(string, map[string]interface{}) error

// Worker represents a RabbitMQ message consumer
type Worker struct {
	conn          *amqp.Connection
	channel       *amqp.Channel
	queue         amqp.Queue
	retryQueue    amqp.Queue
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

	fmt.Println("Declaring main exchange")
	// Declare the main exchange
	err = ch.ExchangeDeclare(
		exchangeName, // Exchange name
		exchangeType, // Exchange type
		true,         // Durable
		false,        // Auto-deleted
		false,        // Internal
		false,        // No-wait
		nil,          // Arguments
	)
	fmt.Println("Declaring main exchange done", err)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to declare an exchange: %w", err)
	}

	fmt.Println("Declaring delay exchange")
	// Declare the delay exchange for retries
	err = ch.ExchangeDeclare(
		delayExchangeName, // Exchange name
		exchangeType,      // Exchange type (direct)
		true,              // Durable
		false,             // Auto-deleted
		false,             // Internal
		false,             // No-wait
		nil,               // Arguments
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to declare delay exchange: %w", err)
	}

	// Declare the main queue with dead letter config
	args := amqp.Table{
		"x-dead-letter-exchange":    delayExchangeName,
		"x-dead-letter-routing-key": retryQueueName,
	}
	q, err := ch.QueueDeclare(
		queueName,
		true,  // Durable
		false, // Auto delete
		false, // Exclusive
		false, // No-wait
		args,  // Arguments with DLX config
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to declare main queue: %w", err)
	}

	// Declare retry queue with TTL and dead letter config
	retryArgs := amqp.Table{
		"x-dead-letter-exchange":    exchangeName,
		"x-dead-letter-routing-key": queueName,
		"x-message-ttl":             int32(retryDelay.Milliseconds()),
	}
	retryQueue, err := ch.QueueDeclare(
		retryQueueName,
		true,  // Durable
		false, // Auto delete
		false, // Exclusive
		false, // No-wait
		retryArgs,
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to declare retry queue: %w", err)
	}

	// Bind the main queue to the main exchange
	err = ch.QueueBind(
		q.Name,       // Queue name
		q.Name,       // Routing key (same as queue name for direct exchange)
		exchangeName, // Exchange
		false,        // No-wait
		nil,          // Arguments
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to bind queue to exchange: %w", err)
	}

	// Bind the retry queue to the delay exchange
	err = ch.QueueBind(
		retryQueue.Name,   // Queue name
		retryQueue.Name,   // Routing key
		delayExchangeName, // Exchange
		false,             // No-wait
		nil,               // Arguments
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to bind retry queue to delay exchange: %w", err)
	}

	return &Worker{
		conn:          conn,
		channel:       ch,
		queue:         q,
		retryQueue:    retryQueue,
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
			retryCount := getRetryCount(msg.Headers)
			extra_data := map[string]interface{}{
				"retry_count": retryCount,
			}
			err := w.processMessage(msg, extra_data)

			if err != nil {
				// Handle failed processing
				if retryCount < maxRetries {
					w.scheduleRetry(msg, retryCount+1)
					fmt.Printf("Message processing failed, scheduled retry %d of %d in %v\n",
						retryCount+1, maxRetries, retryDelay)
				} else {
					fmt.Printf("Message processing failed after %d retries, discarding\n", maxRetries)
					// Could send to a dead letter queue for manual inspection here
				}
				msg.Ack(false) // Remove from the queue regardless of success
			} else {
				// Process succeeded
				msg.Ack(false)
			}
		}
	}()

	fmt.Println(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever

	return nil
}

// processMessage handles an incoming message
func (w *Worker) processMessage(msg amqp.Delivery, extra_data map[string]interface{}) error {
	// Call the handler function with the message body
	if w.handleMessage != nil {
		return w.handleMessage(string(msg.Body), extra_data)
	}
	return nil
}

// getRetryCount extracts the retry count from message headers
func getRetryCount(headers amqp.Table) int {
	if headers == nil {
		return 0
	}

	if retryCount, exists := headers["x-retry-count"]; exists {
		switch count := retryCount.(type) {
		case int:
			return count
		case int32:
			return int(count)
		case int64:
			return int(count)
		case string:
			if i, err := strconv.Atoi(count); err == nil {
				return i
			}
		}
	}

	return 0
}

// scheduleRetry republishes a message for retry with updated retry count
func (w *Worker) scheduleRetry(msg amqp.Delivery, retryCount int) {
	// Prepare headers for the retry
	headers := amqp.Table{}

	// Copy existing headers
	if msg.Headers != nil {
		for k, v := range msg.Headers {
			headers[k] = v
		}
	}

	// Update retry count
	headers["x-retry-count"] = retryCount

	// Publish to the dead letter exchange to trigger the delay mechanism
	err := w.channel.Publish(
		"",                // Exchange (default exchange)
		w.retryQueue.Name, // Routing key (queue name)
		false,             // Mandatory
		false,             // Immediate
		amqp.Publishing{
			Headers:         headers,
			ContentType:     msg.ContentType,
			ContentEncoding: msg.ContentEncoding,
			Body:            msg.Body,
			DeliveryMode:    msg.DeliveryMode, // Persistent/transient message
		},
	)

	if err != nil {
		log.Printf("Failed to schedule retry: %v", err)
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
