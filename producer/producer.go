// This is a simple Kafka producer that simulates streaming sensor data into a Kafka topic called "sensor-events".
// It generates 40 sensor events with random temperature readings and sends them to Kafka with a short delay between each event.
// The producer uses the sensor name as the key, which ensures that all events from the same sensor go to the same partition.

package main

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	kafka "github.com/segmentio/kafka-go" // Kafka client library for Go
)

func main() {
	// Create a Kafka writer that connects to the local Kafka cluster and writes to the "sensor-events" topic
	writer := &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Topic:    "sensor-events",
		Balancer: &kafka.Hash{}, // routes by key; same sensor always goes to the same partition
	}
	defer writer.Close()

	sensors := []string{"FOX1", "FOX2", "FOX3", "FOX4"}

	fmt.Println("=== PRODUCER 1 STARTED: Streaming sensor data into Kafka ===")

	// Simulate streaming 20 sensor events with a short delay between them
	for i := 0; i < 20; i++ {
		sensor := sensors[i%4]     // rotate through the 4 sensors
		temp := rand.Intn(30) + 60 // random temperature between 60 and 90
		// Create a JSON-formatted message with the event number, sensor name, and temperature reading
		msg := fmt.Sprintf(`{"event": %d, "sensor": "%s", "temp_F": %d}`, i, sensor, temp)

		// Write the message to Kafka; the key is the sensor name, which determines the partition
		err := writer.WriteMessages(context.Background(),
			kafka.Message{
				Key:   []byte(sensor), // key determines which partition receives this message
				Value: []byte(msg),
			},
		)
		if err != nil {
			fmt.Printf("Failed to send: %v\n", err)
		} else {
			fmt.Printf("[Producer 1] Sent -> %s\n", msg)
		}

		time.Sleep(200 * time.Millisecond) // simulate delay between sent events
	}

	fmt.Println("=== PRODUCER 1 DONE ===")
}
