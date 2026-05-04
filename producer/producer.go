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

// SensorBalancer explicitly maps each sensor name to a fixed partition,
// guaranteeing one sensor per partition with no hash collisions.
type SensorBalancer struct {
	partitionMap map[string]int
}

func (b *SensorBalancer) Balance(msg kafka.Message, partitions ...int) int {
	if partition, ok := b.partitionMap[string(msg.Key)]; ok {
		return partition
	}
	return partitions[0] // fallback for unknown keys
}

func main() {
	// Create a Kafka writer that connects to the local Kafka cluster and writes to the "sensor-events" topic
	writer := &kafka.Writer{
		Addr:  kafka.TCP("localhost:9092"),
		Topic: "sensor-events",
		Balancer: &SensorBalancer{
			partitionMap: map[string]int{
				"FOX":    0,
				"RABBIT": 1,
				"SNAKE":  2,
				"OCELOT": 3,
			},
		},
	}
	defer writer.Close()

	sensors := []string{"FOX", "RABBIT", "SNAKE", "OCELOT"}

	fmt.Println("=== PRODUCER 1 STARTED: Streaming sensor data into Kafka ===")

	// Simulate streaming 8 sensor events with a short delay between them
	for i := 0; i < 8; i++ {
		sensor := sensors[i%4]     // rotate through the 4 sensors
		temp := rand.Intn(30) + 60 // random temperature between 60 and 90; it's not important that these are realistic, just that they vary
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
