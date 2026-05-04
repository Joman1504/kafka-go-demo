// This is a Kafka consumer that demonstrates how to use consumer groups to achieve parallel processing of messages across multiple goroutines.
// Instead of each goroutine reading from a specific partition, all goroutines share the same consumer group ID.
// Kafka will automatically load balance messages across the goroutines, ensuring that each message is processed by only one goroutine.
// This allows us to easily scale our consumer by simply adding more goroutines with the same group ID.

package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	kafka "github.com/segmentio/kafka-go" // Kafka client library for Go
)

// Each goroutine runs this function independently and concurrently.
// instanceID identifies which process this is (passed via CLI arg or env var).
// workerID is the index of this goroutine within the process (0, 1, 2, ...).
// With a GroupID, Kafka dynamically assigns partitions — workerID is NOT a partition number.
func consumeMessages(ctx context.Context, instanceID string, workerID int, wg *sync.WaitGroup) {
	defer wg.Done() // signal that this goroutine is done when the function returns

	label := fmt.Sprintf("Goroutine %d", workerID)
	if instanceID != "" {
		label = fmt.Sprintf("%s/Goroutine %d", instanceID, workerID)
	}

	// Create a Kafka reader that connects to the local Kafka cluster and reads from the "sensor-events" topic.
	// Because GroupID is set (not Partition), Kafka will dynamically assign partitions to this reader.
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    "sensor-events",
		GroupID:  "sensor-processor-group", // all 1s share the same group ID, so Kafka will automatically load balance messages across them
		MinBytes: 1,
		MaxBytes: 10e6,
	})
	defer reader.Close()

	// // Start reading from the beginning so we don't miss messages
	// reader.SetOffset(kafka.FirstOffset)
	reader.SetOffset(kafka.LastOffset) // start at the end to only get new messages

	// Note: with GroupID, Kafka decides which partition(s) this worker will read from.
	// The actual partition number appears in each message below.
	fmt.Printf("[%s] Ready - waiting for Kafka partition assignment\n", label)

	// This loop runs indefinitely until the context is cancelled (Ctrl+C)
	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			// ctx was canceled (Ctrl+C), so we exit cleanly
			fmt.Printf("[%s] Shutting down.\n", label)
			return
		}

		// Simulate real processing work (e.g. writing to a database, running analytics, etc.)
		fmt.Printf("[%s] <- PROCESSING kafka-partition=%d | offset=%3d | %s\n", label, msg.Partition, msg.Offset, string(msg.Value))
		time.Sleep(500 * time.Millisecond) // simulate time-consuming processing
		fmt.Printf("[%s]    DONE       kafka-partition=%d | offset=%3d\n", label, msg.Partition, msg.Offset)
	}
}

func main() {
	// Use an INSTANCE_ID env var (e.g. "A", "B") to distinguish multiple running processes.
	// If not set, labels will just be "worker-N".
	instanceID := os.Getenv("INSTANCE_ID")

	// This context is cancelled when Ctrl+C is pressed
	ctx, cancel := context.WithCancel(context.Background())

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		fmt.Println("\nCtrl+C detected, shutting down all goroutines...")
		cancel() // signal all goroutines to stop
	}()

	// Start one worker goroutine per partition. With GroupID, Kafka will spread the
	// 4 partitions across however many total workers exist (across all running instances).
	numWorkers := 4
	var wg sync.WaitGroup

	fmt.Printf("=== CONSUMER STARTED (instance: %s) ===\n", instanceID)
	fmt.Printf("Launching %d worker goroutines - Kafka will assign partitions dynamically\n\n", numWorkers)

	// This is the parallel processing part: goroutines start simultaneously
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go consumeMessages(ctx, instanceID, i, &wg) // "go" keyword = new goroutine = parallel execution
	}

	// Wait for all goroutines to finish
	wg.Wait()

	fmt.Println("\n=== ALL GOROUTINES COMPLETE ===")
}
