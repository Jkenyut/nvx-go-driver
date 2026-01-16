# nvx-go-driver

![Go Version](https://img.shields.io/badge/go-1.21%2B-blue)
![License](https://img.shields.io/badge/license-private-red)

**nvx-go-driver** is a production-ready driver wrapper for Go, designed for high-availability microservices. It provides robust clients for PostgreSQL, Redis, RabbitMQ, and Kafka with built-in observability, zero-downtime reconnection, and structured logging.

## Features

- **Standardized API**: Consistent `NewClient(config, logger)` pattern across all drivers.
- **Resilience**: Auto-reconnect logic customized for each protocol (PGX Pool, RabbitMQ Reconnect Loop, etc.).
- **Observability**: Built-in Prometheus-compatible metrics.
- **Structured Logging**: Integrated with `zerolog`.
- **Smart Defaults**: Minimal configuration needed (e.g., just `Enable: true` works for localhost).

## Installation

```bash
go get github.com/Jkenyut/nvx-go-driver
```

## Quick Start (PostgreSQL)

```go
package main

import (
	"context"
	"os"

	"github.com/Jkenyut/nvx-go-driver/config"
	"github.com/Jkenyut/nvx-go-driver/postgres"
	"github.com/rs/zerolog"
)

func main() {
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	cfg := config.SQLConfig{
		Enable:   true,
		Host:     "localhost",
		Username: "postgres",
		Password: "password",
		Database: "mydb",
	}

	dbClient, err := postgres.NewClient(cfg, &logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to connect")
	}
	defer dbClient.Close()
    // Use dbClient.Pool() ...
}
```

## Supported Drivers

### 1. Redis

Backed by `go-redis/v9`.

```go
import "github.com/Jkenyut/nvx-go-driver/redis"

cfg := config.RedisConfig{
    Enable: true,
    Host:   "localhost",
    // Port defaults to 6379
}
// Shortcut methods (Simple Set/Get)
err := client.Set(ctx, "key", "value", 0)
val, err := client.Get(ctx, "key")

```

| Config | Default | Description |
| :--- | :--- | :--- |
| `PoolSize` | `10` | Max connections in pool |
| `MinIdleConn` | `5` | Min idle connections |

### 2. RabbitMQ

Backed by `amqp091-go` with **infinite auto-reconnect loop**.

```go
import "github.com/Jkenyut/nvx-go-driver/rabbitmq"

cfg := config.RabbitMQConfig{
    Enable: true,
    Host:   "localhost",
    // Auto-reconnects every 5s if lost
}
mq, err := rabbitmq.NewClient(cfg, logger)

// Shortcut: Fire-and-forget publish
err := mq.Publish(ctx, "amq.topic", "routing.key", []byte("hello"))

// Or use raw channel for advanced usage
ch, err := mq.Channel()
```

### 3. Kafka

Backed by `IBM/sarama`. Supports PLAIN and SASL/SSL.

```go
import "github.com/Jkenyut/nvx-go-driver/kafka"
// ...
kafkaFactory, err := kafka.NewClient(cfg, logger)

// Shortcut: Simple Publish
err := kafkaFactory.Publish(ctx, "my-topic", []byte("message"))

// Advanced: Custom Producer/Consumer
producer, _ := kafkaFactory.NewAsyncProducer()
```

## Shortcuts Overview

We provide high-level methods to avoid accessing low-level clients for common tasks:

| Driver | Shortcut Methods |
| :--- | :--- |
| **Database** | `Exec`, `Query`, `QueryRow`, `Begin` |
| **Redis** | `Set`, `Get`, `Del` |
| **RabbitMQ** | `Publish` |
| **Kafka** | `Publish` |

## Configuration

All configurations are defined in `config/config.go`. You can load them easily using `cleanenv` or standard JSON/YAML unmarshallers.

## Observability

All clients expose a `Metrics()` method returning structs suitable for Prometheus collectors.

```go
// Example: RabbitMQ does not expose pool metrics, but Redis and PGX do.
redisMetrics := redisClient.Metrics()
pgxMetrics := dbClient.Metrics()
```
