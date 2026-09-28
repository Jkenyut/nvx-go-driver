# Technical Documentation & Architecture Reference

## Overview
`nvx-go-driver` is a production-grade infrastructure client library built for Go 1.22+. It unifies connection pooling, reconnection resilience, distributed tracing, metrics, and configuration for PostgreSQL (`pgx/v5`), Redis (`go-redis/v9`), RabbitMQ (`amqp091-go`), and Kafka (`segmentio/kafka-go`).

---

## 1. System Architecture

```mermaid
graph TD
    App[Application Code]
    
    subgraph ConfigLayer["Configuration Layer (Milliseconds Standard)"]
        SQLCfg["SQLConfig<br/>(connect_timeout_ms, max_conn_lifetime_ms, ...)"]
        RedisCfg["RedisConfig<br/>(connectTimeoutMs, poolTimeoutMs, connMaxLifeMs, ...)"]
        RBCfg["RabbitMQConfig<br/>(connectTimeoutMs, reconnectDurationMs, publishTimeoutMs)"]
        KafkaCfg["KafkaConfig<br/>(Brokers, SecurityProtocol, ...)"]
    end

    subgraph OptionPattern["Functional Options"]
        SQLOpts["postgres.Option<br/>- WithLogger<br/>- WithAfterConnect<br/>- WithBeforeConnect"]
        RedisOpts["redis.Option<br/>- WithLogger"]
        RBOpts["rabbitmq.Option / ConsumerOption / PublisherOption<br/>- WithLogger<br/>- WithConsumerQos<br/>- WithAutoAck<br/>- WithMaxAttempts"]
        KafkaOpts["kafka.Option<br/>- WithLogger<br/>- WithDialer"]
    end

    subgraph Drivers["Resilient Driver Core"]
        PGClient["postgres.Client<br/>(Atomic Pool Swap, Health Monitor)"]
        RedisClient["redis.Client<br/>(Exponential Retry Ping, JSON Helpers)"]
        RabbitClient["rabbitmq.Client<br/>(Reconnect Loop, Confirm Listener)"]
        KafkaClient["kafka.Client<br/>(Smart Murmur2/LeastBytes, Dialer Pool)"]
    end

    subgraph ExternalServices["External Infrastructure"]
        PGServer[("PostgreSQL")]
        RedisServer[("Redis")]
        RabbitServer[("RabbitMQ Broker")]
        KafkaBrokers[("Kafka Cluster")]
    end

    App -->|Reads| ConfigLayer
    App -->|Configures via| OptionPattern
    OptionPattern --> Drivers
    ConfigLayer --> Drivers

    PGClient -->|pgxpool| PGServer
    RedisClient -->|go-redis| RedisServer
    RabbitClient -->|amqp091| RabbitServer
    KafkaClient -->|kafka-go| KafkaBrokers
```

---

## 2. PostgreSQL Connection Lifecycle & Pool Swapping

```mermaid
sequenceDiagram
    autonumber
    participant App as Application
    participant Client as postgres.Client
    participant Pool as pgxpool.Pool (Current)
    participant Monitor as Background Monitor
    participant DB as PostgreSQL Server

    App->>Client: NewClient(cfg, postgres.WithLogger(log), postgres.WithAfterConnect(hook))
    Client->>Client: cfg.WithDefaults() (converts ms to time.Duration)
    Client->>DB: Dial initial pgxpool
    DB-->>Client: Connection OK
    Client->>Client: setPool(activePool), set healthy=true
    Client-->>App: *Client ready

    par Background Health Loop
        loop Every cfg.HealthCheckPeriod (ms)
            Monitor->>Client: Pool().Ping(ctx)
            alt Ping Successful
                Client-->>Monitor: OK (healthy=true)
            else Ping Fails / Connection Severed
                Client->>Client: healthy=false
                Monitor->>DB: Reconnect with Exponential Jitter Backoff
                DB-->>Monitor: New Pool Ready
                Monitor->>Client: Atomic Swap: setPool(newPool)
                Monitor->>Pool: Drain & Close old pool
                Client->>Client: healthy=true
            end
        end
    and Application Queries
        App->>Client: Query / Exec / RunInTx
        Client->>Pool: Forward to atomic active pool
        Pool->>DB: Execute query
        DB-->>App: Rows / CommandTag
    end
```

---

## 3. RabbitMQ Reconnection & Publisher Confirm Sequence

```mermaid
sequenceDiagram
    autonumber
    participant Pub as Publisher
    participant Client as rabbitmq.Client
    participant Broker as RabbitMQ Broker

    App->>Client: NewClient(cfg, rabbitmq.WithLogger(log))
    Client->>Broker: DialConfig (connectTimeoutMs)
    Broker-->>Client: AMQP Connection Established
    Client->>Client: start reconnectLoop()

    App->>Pub: NewPublisher(client, rabbitmq.WithMaxAttempts(3))
    Pub->>Client: NewChannel()
    Client->>Broker: Channel.Confirm()
    Broker-->>Pub: Confirm Mode Active

    rect rgb(240, 248, 255)
    Note over Pub,Broker: Publishing with Confirmation
    Pub->>Broker: BasicPublish (Body, MessageId, Timestamp)
    Pub->>Pub: Track deliveryTag in pendingConfirms map
    alt Broker ACKs
        Broker-->>Pub: Ack(deliveryTag)
        Pub->>Pub: Resolve confirm channel (nil)
    else Network Drops / Nack
        Broker--xPub: Channel / Connection Lost
        Pub->>Pub: Reject pending with error & trigger retry up to maxAttempts
    end
    end

    rect rgb(255, 245, 245)
    Note over Client,Broker: Automatic Reconnection
    Broker--xClient: NotifyClose triggered
    Client->>Client: ready = false
    loop Every reconnectDurationMs with Backoff
        Client->>Broker: Re-dial connection
        Broker-->>Client: Connected!
        Client->>Client: ready = true
    end
    end
```

---

## 4. Configuration Units Migration Reference

All configuration durations have been migrated from raw seconds to explicit milliseconds (`_ms` or `Ms`).

| Struct | Field Name | YAML/JSON Key | Default Value | Equivalent Duration |
| :--- | :--- | :--- | :--- | :--- |
| **`SQLConfig`** | `MaxConnLifetime` | `max_conn_lifetime_ms` | `3600000` | 1 hour |
| | `MaxConnIdleTime` | `max_conn_idle_time_ms` | `600000` | 10 minutes |
| | `HealthCheckPeriod` | `health_check_period_ms` | `15000` | 15 seconds |
| | `ConnectTimeout` | `connect_timeout_ms` | `10000` | 10 seconds |
| **`RedisConfig`** | `StartInterval` | `startIntervalMs` | `2000` | 2 seconds |
| | `PoolTimeout` | `poolTimeoutMs` | `30000` | 30 seconds |
| | `ConnectTimeout` | `connectTimeoutMs` | `5000` | 5 seconds |
| | `ConnMaxLife` | `connMaxLifeMs` | `600000` | 10 minutes |
| **`RabbitMQConfig`**| `ReconnectDuration`| `reconnectDurationMs` | `5000` | 5 seconds |
| | `ConnectTimeout` | `connectTimeoutMs` | `10000` | 10 seconds |
| | `PublishTimeout` | `publishTimeoutMs` | `5000` | 5 seconds |

---

## 5. Functional Options Quick Reference

### PostgreSQL
```go
client, err := postgres.NewClient(&cfg,
    postgres.WithLogger(logger),
    postgres.WithAfterConnect(func(ctx context.Context, conn *pgx.Conn) error {
        _, err := conn.Exec(ctx, "SET timezone = 'UTC'")
        return err
    }),
    postgres.WithBeforeConnect(func(ctx context.Context, connCfg *pgx.ConnConfig) error {
        connCfg.RuntimeParams["application_name"] = "custom-app"
        return nil
    }),
)
```

### Redis
```go
client, err := redis.NewClient(&cfg,
    redis.WithLogger(logger),
)
```

### RabbitMQ
```go
// Client
client, err := rabbitmq.NewClient(&cfg,
    rabbitmq.WithLogger(logger),
)

// Consumer with functional options
consumer := rabbitmq.NewConsumer(client, "orders-queue",
    rabbitmq.WithConsumerQos(20),
    rabbitmq.WithAutoAck(false),
    rabbitmq.WithExclusive(false),
)

// Publisher with functional options
publisher, err := rabbitmq.NewPublisher(client,
    rabbitmq.WithMaxAttempts(5),
)
```

### Kafka
```go
client, err := kafka.NewClient(&cfg,
    kafka.WithLogger(logger),
    kafka.WithDialer(customDialer),
)
```
