package main

import (
	"send_message_worker/pkg"
	"send_message_worker/tasks"
)

func main() {
	// Create a new worker with the ProcessComplete handler
	worker, err := pkg.NewWorker(tasks.ProcessComplete)
	pkg.FailOnError(err, "Failed to create worker")
	defer worker.Close()

	// Start consuming messages
	err = worker.StartConsuming()
	pkg.FailOnError(err, "Failed to start consuming")
}
