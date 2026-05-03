// This is a simple Kafka consumer that demonstrates how to read from multiple partitions in parallel using goroutines.
// Each goroutine is responsible for consuming messages from a specific partition of the "sensor-events" topic.
// The consumer will run indefinitely until you press Ctrl+C, at which point it will shut down gracefully.

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

// Each goroutine runs this function independently and concurrently
func consumePartition(ctx context.Context, partitionId int, wg *sync.WaitGroup) {
	defer wg.Done() // signal that this goroutine is done when the function returns

	// Create a Kafka reader that connects to the local Kafka cluster and reads from the "sensor-events" topic, specific to the given partition
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"localhost:9092"},
		Topic:     "sensor-events",
		Partition: partitionId, // read from a specific partition
		MinBytes:  1,
		MaxBytes:  10e6,
	})
	defer reader.Close()

	// // Start reading from the beginning so we don't miss messages
	// reader.SetOffset(kafka.FirstOffset)
	reader.SetOffset(kafka.LastOffset) // start at the end to only get new messages

	fmt.Printf("[Goroutine %d] Ready - listening on partition %d\n", partitionId, partitionId)

	// This loop runs indefinitely until the context is cancelled (Ctrl+C)
	for {

		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			// ctx was canceled (Ctrl+C), so we exit cleanly
			fmt.Printf("[Goroutine %d] Partition %d shutting down.\n", partitionId, partitionId)
			return
		}

		// Simulate real processing work (e.g. writing to a database, running analytics, etc.)
		fmt.Printf("[Goroutine %d] <- PROCESSING Partition %d: | Offset %3d | %s\n", partitionId, msg.Partition, msg.Offset, string(msg.Value))
		time.Sleep(500 * time.Millisecond) // simulate time-consuming processing
		fmt.Printf("[Goroutine %d] DONE   Partition %d: | Offset %3d \n", partitionId, msg.Partition, msg.Offset)
	}
}

func main() {
	// This context is cancelled when Ctrl+C is pressed
	ctx, cancel := context.WithCancel(context.Background())

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		fmt.Println("\nCtrl+C detected, shutting down all goroutines...")
		cancel() // signal all goroutines to stop
	}()

	numPartitions := 4 // we know our topic has 4 partitions, so we'll start 4 goroutines
	var wg sync.WaitGroup

	fmt.Println("=== CONSUMER STARTED ===")
	fmt.Printf("Launching %d goroutines - one per Kafka partition\n\n", numPartitions)

	// This is the parallel processing part: 4 goroutines start simultaneously
	for i := 0; i < numPartitions; i++ {
		wg.Add(1)
		go consumePartition(ctx, i, &wg) // "go" keyword = new goroutine = parallel execution
	}

	// Wait for all goroutines to finish
	wg.Wait()

	fmt.Println("\n=== ALL GOROUTINES COMPLETE ===")
}
