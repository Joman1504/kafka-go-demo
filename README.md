# High-Performance Stream Processing with Apache Kafka and Go

A demonstration project for CS 3700 showing how Apache Kafka and Go's goroutines enable high-performance, parallel stream processing. Two independent producers publish simulated sensor events to a Kafka topic, while consumers process them in parallel — either through explicit partition assignment or Kafka consumer groups.

## Concepts Demonstrated

- **Stream processing** — events are consumed and processed in real time as they arrive, not batched
- **Parallel processing** — one goroutine per Kafka partition processes events simultaneously and independently
- **Consumer groups** — Kafka automatically distributes partitions across multiple consumer instances, enabling fault tolerance and elastic scalability

## Project Structure

```
kafka-demo/
├── docker-compose.yml       # Runs Kafka and ZooKeeper via Docker
├── go.mod
├── go.sum
├── consumer/
│   └── consumer.go          # Explicit partition consumer — one goroutine pinned to each partition
├── consumer2/
│   └── consumer_group.go    # Consumer group — Kafka auto-assigns partitions across instances
├── producer/
│   └── producer.go          # Producer 1 — FOX sensors, steady 200ms interval
└── producer2/
    └── producer2.go         # Producer 2 — RABBIT sensors, random 100–600ms interval
```

## Producers

**Producer 1** (`producer/producer.go`) streams events from four sensors — `FOX1`, `FOX2`, `FOX3`, `FOX4` — at a steady 200ms interval. The sensor name is used as the message key, so Kafka's hash balancer always routes the same sensor to the same partition.

**Producer 2** (`producer2/producer2.go`) streams events from a second set of sensors — `RABBIT1`, `RABBIT2`, `RABBIT3`, `RABBIT4` — at a randomized interval between 100ms and 600ms, simulating uneven real-world data. Both producers write to the same topic independently and simultaneously.

## Consumers

**Consumer** (`consumer/consumer.go`) uses explicit partition assignment. It launches 4 goroutines, each pinned to a specific partition by number. Each goroutine only ever sees events routed to its partition, giving a clear one-to-one relationship between goroutines and partitions.

**Consumer Group** (`consumer2/consumer_group.go`) uses a shared `GroupID` (`sensor-processor-group`). Instead of pinning goroutines to partitions manually, Kafka automatically distributes partitions across however many instances are running. Running a second instance causes Kafka to rebalance — splitting the 4 partitions between both instances. Killing one instance triggers another rebalance, returning all partitions to the survivor.

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) — must be running before starting Kafka
- [Go 1.21+](https://go.dev/dl/)

## Setup

### 1. Start Kafka

```bash
docker compose up -d
```

Wait about 15 seconds for Kafka and ZooKeeper to fully boot, then verify both containers are running:

```bash
docker compose ps
```

### 2. Create the Kafka Topic

The topic requires 4 partitions — one per sensor group:

```bash
docker compose exec kafka kafka-topics \
  --create \
  --topic sensor-events \
  --partitions 4 \
  --replication-factor 1 \
  --bootstrap-server localhost:9092
```

Verify the topic was created:

```bash
docker compose exec kafka kafka-topics \
  --describe --topic sensor-events \
  --bootstrap-server localhost:9092
```

### 3. Install Go Dependencies

```bash
go mod download
```

---

## Running the Demo

### Demo 1 — Explicit Partition Consumer + Two Producers

Open three terminals.

**Terminal 1 — start the consumer first:**
```bash
go run consumer/consumer.go
```

Wait for all four goroutines to print `Ready`, then start both producers.

**Terminal 2 — Producer 1 (FOX sensors, steady pace):**
```bash
go run producer/producer.go
```

**Terminal 3 — Producer 2 (RABBIT sensors, random pace):**
```bash
go run producer2/producer2.go
```

Press `Ctrl+C` in the consumer terminal to shut down cleanly once both producers finish.

---

### Demo 2 — Consumer Groups

Open four terminals.

**Terminal 1 — Consumer Group Instance A:**
```bash
go run consumer2/consumer_group.go
```

**Terminal 2 — Consumer Group Instance B:**
```bash
go run consumer2/consumer_group.go
```

Kafka will split the 4 partitions between the two instances — each handles 2 partitions. Then start both producers in the remaining terminals:

**Terminal 3:**
```bash
go run producer/producer.go
```

**Terminal 4:**
```bash
go run producer2/producer2.go
```

To demonstrate fault tolerance, press `Ctrl+C` in Terminal 1 while events are flowing. Kafka will detect the lost instance and rebalance all 4 partitions to the surviving consumer in Terminal 2.

---

## What to Watch For

- **Goroutine numbers interleaving** in consumer output — parallel execution; goroutines are not taking turns
- **FOX and RABBIT sensors appearing in the same goroutine's output** — two independent producers, one consumer pipeline
- **Consumer continuing after producers finish** — backpressure; events queued in Kafka are worked through in order
- **Partitions splitting across two consumer group instances** — Kafka's automatic load balancing
- **Rebalancing after killing one consumer group instance** — the surviving instance absorbs all 4 partitions automatically

---

## Resetting Between Runs

To clear all events and start fresh:

```bash
docker compose exec kafka kafka-topics \
  --delete --topic sensor-events \
  --bootstrap-server localhost:9092

docker compose exec kafka kafka-topics \
  --create \
  --topic sensor-events \
  --partitions 4 \
  --replication-factor 1 \
  --bootstrap-server localhost:9092
```

## Shutting Down

```bash
docker compose down
```
