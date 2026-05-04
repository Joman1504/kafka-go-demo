// This is a second producer that simulates streaming sensor data into Kafka.

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
				"BERRY":  0,
				"CARROT": 1,
				"APPLE":  2,
				"MELON":  3,
			},
		},
	}
	defer writer.Close()

	sensors := []string{"BERRY", "CARROT", "APPLE", "MELON"}

	fmt.Println("=== PRODUCER 2 STARTED: Streaming sensor data into Kafka ===")

	// Simulate streaming 20 sensor events with a short delay between them
	for i := 0; i < 20; i++ {
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
			fmt.Printf("[Producer 2] Sent -> %s\n", msg)
		}

		// Simulate random delay between sent events (e.g. sensors might not all report at the same time)
		time.Sleep(time.Duration(rand.Intn(500)+100) * time.Millisecond)
	}

	fmt.Println("=== PRODUCER 2 DONE ===")
}
