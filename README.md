# nvx-go-driver

![Go Version](https://img.shields.io/badge/go-1.26%2B-blue)
![License](https://img.shields.io/badge/license-private-red)

**nvx-go-driver** is a production-ready driver wrapper for Go, designed for high-availability microservices. It provides robust clients for PostgreSQL, Redis, RabbitMQ, and Kafka with built-in observability, zero-downtime reconnection, and structured logging.

## Features

- **Standardized API**: Consistent `NewClient(config, logger)` pattern across all drivers.
- **Resilience**: Auto-reconnect logic customized for each protocol (PGX Pool, RabbitMQ Reconnect Loop, Kafka Dialer, etc.).
- **Graceful Shutdown**: Built-in context handling and connection draining to prevent message loss.
- **Observability**: Built-in Prometheus-compatible metrics.
- **Structured Logging**: Integrated with `zerolog` featuring auto-caller and standard log hijacking.
- **Smart Defaults**: Minimal configuration needed (e.g., just `Enable: true` works for localhost).

## Installation

```bash
go get github.com/Jkenyut/nvx-go-driver
```

## Usage Examples

### 1. Logger (Zerolog)

The logger automatically adapts to JSON in production and colored output in development. Standard Go logs are automatically hijacked into Zerolog!

```go
import "github.com/Jkenyut/nvx-go-driver/logger"
// ...
logger.InitFromConfig(cfg.Listener)
defer logger.Close() // Flush remaining logs on shutdown

// Use convenience methods anywhere in your app:
logger.Info().Msg("Application started")
logger.Error().Err(err).Msg("Something failed") // Includes file & line number automatically!
```

### 2. PostgreSQL (Zero-Downtime)

Backed by `pgx/v5`. Supports graceful pool swapping on failure and transaction wrappers.

```go
import "github.com/Jkenyut/nvx-go-driver/postgres"

dbClient, err := postgres.NewClient(cfg.WithDefaults(), logger.L())
defer dbClient.Close()

// Simple query
rows, _ := dbClient.Query(ctx, "SELECT id FROM users")

// Safe Transaction Wrapper (auto rollback on panic/error)
err = dbClient.RunInTx(ctx, func(tx pgx.Tx) error {
    _, err := tx.Exec(ctx, "UPDATE users SET status = 'active'")
    return err
})
```

### 3. RabbitMQ (At-Least-Once Delivery)

Backed by `amqp091-go` with **infinite auto-reconnect**, topology helpers, and idempotency aids.

```go
import "github.com/Jkenyut/nvx-go-driver/rabbitmq"

mq, err := rabbitmq.NewClient(cfg, logger.L())

// Topology Setup Helper
mq.DeclareExchange("my_exchange", "direct", true, false, false, false, nil)
mq.DeclareQueue("my_queue", true, false, false, false, nil)
mq.BindQueue("my_queue", "my_routing_key", "my_exchange", false, nil)

// Publisher (Auto-injects MessageId & Timestamp for Idempotency)
pub, _ := rabbitmq.NewPublisher(mq)
pub.SetMaxAttempts(3) // 3 = At-Least-Once, 1 = At-Most-Once
msg := &amqp091.Publishing{Body: []byte("hello")}
err = pub.Publish(ctx, "my_exchange", "my_routing_key", msg, false, false)

// Consumer (Graceful Shutdown ready)
consumer := rabbitmq.NewConsumer(mq, "my_queue")
consumer.Start(ctx, func(ctx context.Context, msg amqp091.Delivery) rabbitmq.Action {
    fmt.Println(string(msg.Body))
    return rabbitmq.ActionAck // Automatically acks the message
})
defer consumer.Close() // Waits up to 10s for active messages to finish before disconnecting!
```

### 4. Kafka (Segmentio)

Backed by `segmentio/kafka-go`. A highly modern, pure-go driver with native SASL/TLS support.

```go
import "github.com/Jkenyut/nvx-go-driver/kafka"

kafkaClient, err := kafka.NewClient(cfg, logger.L())

// Shortcut: Simple Publish (Synchronous with Partition Key)
err = kafkaClient.Publish(ctx, "my-topic", []byte("user_123"), []byte("payload data"))

// Consumer (Reader)
reader := kafkaClient.NewReader("my-topic", "my-consumer-group")
defer reader.Close()

for {
    msg, err := reader.ReadMessage(ctx)
    if err != nil {
        break // Context cancelled or connection dropped
    }
    fmt.Printf("Received: %s\n", string(msg.Value))
}
```

### 5. Redis

Backed by `go-redis/v9`.

```go
import "github.com/Jkenyut/nvx-go-driver/redis"

redisClient, err := redis.NewClient(cfg, logger.L())
defer redisClient.Close()

// Shortcut methods for quick JSON serialization
err = redisClient.SetJSON(ctx, "user:1", userStruct, time.Hour)
var user User
err = redisClient.GetJSON(ctx, "user:1", &user)
```

## Configuration

All configurations are defined in `config/config.go`. This library provides a smart configuration loader that can:
1. **Auto-generate config files**: If your config file is missing, it will create one with default values.
2. **Auto-repair**: If your config file is missing new fields, it will append them with defaults.

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
redisMetrics := redisClient.Metrics()
pgxMetrics := dbClient.Metrics()
```
