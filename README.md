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

All configurations are defined in `config/config.go`. This library provides a smart configuration loader that can:
1. **Auto-generate config files**: If your config file is missing, it will create one with default values.
2. **Auto-repair**: If your config file is missing new fields, it will append them with defaults.

### Secret Management
We support a **hybrid secret strategy** (File > Env), ideal for Docker Swarm/Kubernetes Secrets with local dev fallback.

```go
// In your config struct
Password: config.SetValueFromEnv(
    "/run/secrets/db_password", // Priority 1: Read from file (Docker Secret)
    "DB_PASSWORD",              // Priority 2: Read from Env (Local Dev)
)
```

### Loading Config
```go
import "github.com/Jkenyut/nvx-go-driver/config"

type AppConfig struct {
    Database config.SQLConfig `yaml:"database"`
}

func main() {
    cfg, err := config.Load[AppConfig]("config.yaml")
    if err != nil {
        panic(err)
    }
    // cfg is now populated. 
    // If config.yaml didn't exist, it was created with defaults!
}
```

## Observability

All clients expose a `Metrics()` method returning structs suitable for Prometheus collectors.

```go
// Example: RabbitMQ does not expose pool metrics, but Redis and PGX do.
redisMetrics := redisClient.Metrics()
pgxMetrics := dbClient.Metrics()
```
