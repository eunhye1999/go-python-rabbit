package tasks

import (
	"context"
	"database/sql"
	"fmt"
	"send_message_worker/pkg"
	"time"
)

func ProcessComplete(msg string) {
	message_id := msg

	hostname := "127.0.0.1"
	database := "messages"
	username := "yourusername"
	password := "yourpassword"
	port := "5432"
	pg, err := pkg.NewPostgres(fmt.Sprintf("postgres://%s:%s@%s:%s/%s", username, password, hostname, port, database))
	if err != nil {
		fmt.Println(err)
	}
	defer pg.Close()

	row, err := pg.ExecuteQueryRow(context.Background(), "SELECT id, message, phone_number, created_on, completed_on FROM messages WHERE id = $1", message_id)
	if err != nil {
		fmt.Println(err)
	}
	var id int
	var message, phone_number string
	var created_on time.Time
	var completed_on sql.NullTime

	// Make sure the number of variables matches the number of fields in your query result
	if err := row.Scan(&id, &message, &phone_number, &created_on, &completed_on); err != nil {
		fmt.Println("Error scanning row:", err)
		return
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

	pg.ExecuteQuery(context.Background(), "UPDATE messages SET completed_on = $1 WHERE id = $2", time.Now(), message_id)
	fmt.Println("Process Completed")
}
