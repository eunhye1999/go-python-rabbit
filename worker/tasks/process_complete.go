package tasks

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"send_message_worker/pkg"
	"time"
)

// RetryableError is a custom error type that indicates the operation should be retried
type RetryableError struct {
	OriginalError error
	Reason        string
}

func (e *RetryableError) Error() string {
	return fmt.Sprintf("Retryable error: %s - %v", e.Reason, e.OriginalError)
}

// NewRetryableError creates a new RetryableError
func NewRetryableError(reason string, err error) *RetryableError {
	return &RetryableError{
		OriginalError: err,
		Reason:        reason,
	}
}

// IsRetryableError checks if an error is a RetryableError
func IsRetryableError(err error) bool {
	var retryErr *RetryableError
	return errors.As(err, &retryErr)
}

// MaxRetryCount defines the maximum number of retries allowed
const MaxRetryCount = 3

func ProcessComplete(msg string, extra_data map[string]interface{}) error {
	message_id := msg
	retry_count := extra_data["retry_count"].(int)
	fmt.Println("-----------------------------------Retry Count:", retry_count, "-----------------------------------")

	// Check if we've exceeded max retries
	if retry_count >= MaxRetryCount {
		return fmt.Errorf("exceeded maximum retry attempts (%d) for message ID %s", MaxRetryCount, message_id)
	}

	hostname := "127.0.0.1"
	database := "messages"
	username := "yourusername"
	password := "yourpassword"
	port := "5432"
	pg, err := pkg.NewPostgres(fmt.Sprintf("postgres://%s:%s@%s:%s/%s", username, password, hostname, port, database))
	if err != nil {
		// Database connection failures are usually temporary and should be retried
		return NewRetryableError("database connection failed", err)
	}
	defer pg.Close()

	row, err := pg.ExecuteQueryRow(context.Background(), "SELECT id, message, phone_number, created_on, completed_on FROM messages WHERE id = $1", message_id)
	if err != nil {
		fmt.Println(err)
		return err
	}
	var id int
	var message, phone_number string
	var created_on time.Time
	var completed_on sql.NullTime

	// Make sure the number of variables matches the number of fields in your query result
	if err := row.Scan(&id, &message, &phone_number, &created_on, &completed_on); err != nil {
		fmt.Println("--------------------------------- Error scanning row:", err, "---------------------------------")
		return err
	}

	// Check if completed_on is NULL or a zero tim
	if !completed_on.Time.IsZero() {
		fmt.Println("Process Already (Completed On is Zero Time)")
	} else {
		// Print the results if completed_on is valid and not zero time
		fmt.Printf("ID: %d\nMessage: %s\nPhone Number: %s\nCreated On: %s\nCompleted On: %s\n",
			id,
			message,
			phone_number,
			created_on.Format("2006-01-02 15:04:05"), // Format the time
			completed_on.Time.Format("2006-01-02 15:04:05"))
	}

	_, err = pg.ExecuteQuery(context.Background(), "UPDATE messages SET completed_on = $1 WHERE id = $2", time.Now(), message_id)
	if err != nil {
		fmt.Println("--------------------------------- Error updating completed_on:", err, "---------------------------------")
		return err
	}

	fmt.Println("--------------------------------- Process Completed ---------------------------------")
	return nil
}
