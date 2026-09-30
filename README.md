# nvx-go-driver

![Go Version](https://img.shields.io/badge/go-1.26%2B-blue)
![License](https://img.shields.io/badge/license-private-red)

**nvx-go-driver** is a production-ready driver wrapper for Go, designed for high-availability microservices. It provides robust clients for PostgreSQL, Redis, RabbitMQ, and Kafka with built-in observability, zero-downtime reconnection, and structured logging.

## Features

- **Standardized API**: Consistent `NewClient(config, logger)` pattern across all drivers.
- **Resilience**: Auto-reconnect logic customized for each protocol (PGX Pool, RabbitMQ Reconnect Loop, Kafka Dialer, etc.).
- **Graceful Shutdown**: Built-in context handling and connection draining to prevent message loss.
- **Observability**: Built-in Prometheus-compatible metrics and native OpenTelemetry distributed tracing.
- **Structured Logging**: Fully agnostic using Go standard library `log/slog`. Accepts any `*slog.Logger` or `nil` (safe no-op via `slog.DiscardHandler`).
- **Smart Defaults**: Minimal configuration needed (e.g., just `Enable: true` works for localhost).

## Installation

```bash
go get github.com/Jkenyut/nvx-go-driver
```

## Usage Examples

### 1. Structured Logging (Go Standard Library `log/slog`)

All drivers accept standard library `*slog.Logger`. You can pass `slog.Default()`, a custom logger, or `nil` (which safely discards logs with zero allocations).

```go
import "log/slog"

// Pass your application logger, or nil for silent mode
log := slog.Default()
```

### 2. PostgreSQL (Zero-Downtime)

Backed by `pgx/v5`. Supports graceful pool swapping on failure and transaction wrappers.

```go
import "github.com/Jkenyut/nvx-go-driver/postgres"

// Functional options: WithLogger, WithAfterConnect, WithBeforeConnect
dbClient, err := postgres.NewClient(&cfg,
    postgres.WithLogger(log),
    postgres.WithAfterConnect(func(ctx context.Context, conn *pgx.Conn) error {
        _, err := conn.Exec(ctx, "SET timezone = 'UTC'")
        return err
    }),
)
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

mq, err := rabbitmq.NewClient(&cfg, rabbitmq.WithLogger(log))

// Topology Setup Helper
mq.DeclareExchange("my_exchange", "direct", true, false, false, false, nil)
mq.DeclareQueue("my_queue", true, false, false, false, nil)
mq.BindQueue("my_queue", "my_routing_key", "my_exchange", false, nil)

// Publisher with options (WithMaxAttempts)
pub, _ := rabbitmq.NewPublisher(mq, rabbitmq.WithMaxAttempts(3))
msg := &amqp091.Publishing{Body: []byte("hello")}
err = pub.Publish(ctx, "my_exchange", "my_routing_key", msg, false, false)

// Consumer with functional options (WithConsumerQos, WithAutoAck, etc.)
consumer := rabbitmq.NewConsumer(mq, "my_queue",
    rabbitmq.WithConsumerQos(10),
    rabbitmq.WithAutoAck(false),
)
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

kafkaClient, err := kafka.NewClient(&cfg, kafka.WithLogger(log))

// Shortcut: Simple Publish (Synchronous with Partition Key)
err = kafkaClient.Publish(ctx, "my-topic", []byte("user_123"), []byte("payload data"))

// Consumer (Reader)
reader := kafkaClient.NewReader("my-topic", "my-consumer-group")
defer reader.Close()
```

### 5. Redis

Backed by `go-redis/v9`.

```go
import "github.com/Jkenyut/nvx-go-driver/redis"

redisClient, err := redis.NewClient(&cfg, redis.WithLogger(log))
defer redisClient.Close()

// Shortcut methods for quick JSON serialization
err = redisClient.SetJSON(ctx, "user:1", userStruct, time.Hour)
var user User
err = redisClient.GetJSON(ctx, "user:1", &user)
```

## Architecture & Diagrams

Detailed system architecture diagrams, lifecycle sequences, and migration specifications can be found in [ARCHITECTURE.md](file:///Users/satria/workspace/projects/nvx-go/nvx-go-driver/ARCHITECTURE.md).

## Configuration (Milliseconds Standard)

All time duration configurations are specified in **milliseconds** (`_ms` or `Ms`).
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

### 1. Prometheus Metrics
All clients expose a `Metrics()` method returning structs suitable for Prometheus collectors.

```go
redisMetrics := redisClient.Metrics()
pgxMetrics := dbClient.Metrics()
```

### 2. Distributed Tracing (OpenTelemetry)
PostgreSQL client provides automatic query tracing with OpenTelemetry. Enable it via configuration:

```yaml
database:
  enable: true
  enable_telemetry: true # Enables automatic OpenTelemetry spans for SQL queries
```

When enabled, query spans (`db.client`) are automatically created using `otel.Tracer("nvx-go-driver/postgres")` with sanitized SQL query statements, statement types, and database metadata attributes.

