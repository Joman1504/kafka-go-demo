# High-Performance Stream Processing with Apache Kafka and Go

A demonstration project for CS 3700 showing how Apache Kafka and Go's goroutines enable high-performance, parallel stream processing. Two independent producers publish simulated sensor events to a Kafka topic, while a consumer processes them in parallel using one goroutine per partition.

## Concepts Demonstrated

- **Stream processing** — events are consumed and processed in real time as they arrive, not batched
- **Parallel processing** — one goroutine per Kafka partition processes events simultaneously and independently
- **Backpressure handling** — if a goroutine is busy, new events queue safely in Kafka's partition log until it is ready
- **Producer decoupling** — multiple producers write to the same topic without knowing about each other; the consumer requires no changes

## Project Structure

```
kafka-demo/
├── docker-compose.yml   # Runs Kafka and ZooKeeper via Docker
├── go.mod
├── go.sum
├── consumer/
│   └── consumer.go      # Parallel consumer — one goroutine per partition
├── producer/
│   └── producer.go      # Producer 1 — sensors A through D
└── producer2/
    └── producer2.go     # Producer 2 — machines 1 through 4
```

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

The topic requires 4 partitions — one per sensor/goroutine:

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

## Running the Demo

Open three terminal windows and run each command in its own terminal.

**Terminal 1 — start the consumer first:**
```bash
go run consumer/consumer.go
```

Wait for all four goroutines to print `Ready`, then start the producers.

**Terminal 2 — Producer 1** (sensors A–D):
```bash
# Steady pace
go run producer/producer.go

# Random bursts
go run producer/producer.go -burst
```

**Terminal 3 — Producer 2** (machines 1–4):
```bash
# Steady pace
go run producer2/producer2.go

# Random bursts
go run producer2/producer2.go -burst
```

Press `Ctrl+C` in the consumer terminal to shut it down cleanly once the producers finish.

## What to Watch For

- **Goroutine numbers interleaving** in consumer output (`[Goroutine 0]`, `[Goroutine 2]`, `[Goroutine 1]`...) — this is parallel execution; they are not taking turns
- **Sensor/machine names staying within the same goroutine** — the custom balancer guarantees each key always routes to the same partition
- **Events from both producers appearing in the same goroutine** — two independent producers writing to the same partition simultaneously
- **Consumer continuing after producers finish** (especially with `-burst`) — this is backpressure; events queued in Kafka are being worked through in order

## Resetting Between Runs

To clear all events from the topic and start fresh:

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
