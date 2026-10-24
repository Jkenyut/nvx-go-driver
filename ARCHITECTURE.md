# Technical Documentation and Architecture Reference

## Overview
The `nvx-go-driver` library provides infrastructure client drivers for Go applications. It unifies connection pooling, reconnection resilience, distributed tracing, and configuration parameters for PostgreSQL, Redis, RabbitMQ, and Kafka.

---

## 1. System Architecture

```mermaid
flowchart TD
    App["Application Code"]
    
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

    PGClient -->|"pgxpool (TCP)"| PGServer
    RedisClient -->|"go-redis (TCP)"| RedisServer
    RabbitClient -->|"amqp091 (TCP)"| RabbitServer
    KafkaClient -->|"kafka-go (TCP)"| KafkaBrokers
```

---

## 2. PostgreSQL Connection Lifecycle and Pool Swapping

```mermaid
sequenceDiagram
    autonumber
    participant App as Application Service
    participant Client as postgres.Client
    participant Pool as pgxpool.Pool (Current)
    participant Monitor as Background Monitor Loop
    participant DB as PostgreSQL Server

    App->>Client: Call NewClient(cfg, postgres.WithLogger(log))
    Client->>Client: Execute cfg.WithDefaults() (converts ms to Duration)
    Client->>DB: Dial initial pgxpool connection
    DB-->>Client: Connection successful
    Client->>Client: Call setPool(activePool), healthy = true
    Client-->>App: Return Client pointer

    par Background Health Loop
        loop Every cfg.HealthCheckPeriod (ms)
            Monitor->>Client: Send Pool().Ping(ctx)
            alt Ping succeeds
                Client-->>Monitor: Return OK (healthy = true)
            else Ping fails
                Client->>Client: Set healthy = false
                Monitor->>DB: Reconnect with Exponential Jitter Backoff
                DB-->>Monitor: New pool connection ready
                Monitor->>Client: Atomic swap: setPool(newPool)
                Monitor->>Pool: Drain and close old pool after delay
                Client->>Client: Set healthy = true
            end
        end
    and Application Queries
        App->>Client: Execute Query / Exec / RunInTx
        Client->>Pool: Forward to atomic active pool
        Pool->>DB: Execute SQL query
        DB-->>App: Return rows or CommandTag
    end
```

### Procedural Rules for Database Connections (ASD-STE100)
1. Initialize the PostgreSQL client using `postgres.NewClient`.
2. Access the active database pool through `client.Pool()`.
3. Wrap transactional operations with `client.RunInTx` for automatic rollback safety.
4. Close the client using `client.Close()` when the application shuts down.

---

## 3. RabbitMQ Reconnection and Publisher Confirm Sequence

```mermaid
sequenceDiagram
    autonumber
    participant App as Application Service
    participant Pub as rabbitmq.Publisher
    participant Client as rabbitmq.Client
    participant Broker as RabbitMQ Broker

    App->>Client: Call NewClient(cfg, rabbitmq.WithLogger(log))
    Client->>Broker: DialConfig with connectTimeoutMs
    Broker-->>Client: AMQP connection established
    Client->>Client: Start reconnectLoop()

    App->>Pub: Call NewPublisher(client, rabbitmq.WithMaxAttempts(3))
    Pub->>Client: Request NewChannel()
    Client->>Broker: Enable channel confirmation mode
    Broker-->>Pub: Confirm mode active

    rect rgb(240, 248, 255)
    Note over Pub,Broker: Publishing with Confirmation
    Pub->>Broker: Send BasicPublish (Body, MessageId, Timestamp)
    Pub->>Pub: Track deliveryTag in pendingConfirms map
    alt Broker returns Ack
        Broker-->>Pub: Deliver Ack(deliveryTag)
        Pub->>Pub: Resolve confirm channel with nil error
    else Connection drops or Broker returns Nack
        Broker--xPub: Channel or connection lost
        Pub->>Pub: Reject pending confirmations and trigger retry
    end
    end

    rect rgb(255, 245, 245)
    Note over Client,Broker: Automatic Reconnection
    Broker--xClient: NotifyClose triggered
    Client->>Client: Set ready = false
    loop Every reconnectDurationMs with Backoff
        Client->>Broker: Re-dial connection
        Broker-->>Client: Connection restored
        Client->>Client: Set ready = true
    end
    end
```

---

## 4. Configuration Units Reference

All configuration durations use explicit milliseconds (`_ms` or `Ms`).

| Struct | Field Name | YAML/JSON Key | Default Value | Notes / Equivalent Duration |
| :--- | :--- | :--- | :--- | :--- |
| **`SQLConfig`** | `MaxConnLifetime` | `max_conn_lifetime_ms` | `3600000` | 1 hour |
| | `MaxConnIdleTime` | `max_conn_idle_time_ms` | `600000` | 10 minutes |
| | `HealthCheckPeriod` | `health_check_period_ms` | `15000` | 15 seconds |
| | `ConnectTimeout` | `connect_timeout_ms` | `10000` | 10 seconds |
| | `EnableTelemetry` | `enable_telemetry` | `false` | Enables OpenTelemetry SQL query spans |
| | `Schema` | `schema` | `"public"` | PostgreSQL `search_path` schema |
| **`RedisConfig`** | `StartInterval` | `startIntervalMs` | `2000` | 2 seconds |
| | `PoolTimeout` | `poolTimeoutMs` | `30000` | 30 seconds |
| | `ConnectTimeout` | `connectTimeoutMs` | `5000` | 5 seconds |
| | `ConnMaxLife` | `connMaxLifeMs` | `600000` | 10 minutes |
| **`RabbitMQConfig`**| `ReconnectDuration`| `reconnectDurationMs` | `5000` | 5 seconds |
| | `ConnectTimeout` | `connectTimeoutMs` | `10000` | 10 seconds |
| | `PublishTimeout` | `publishTimeoutMs` | `5000` | 5 seconds |

---

## 5. Functional Options Quick Reference

### 5.1 PostgreSQL Client Options
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

### 5.2 Redis Client Options
```go
client, err := redis.NewClient(&cfg,
    redis.WithLogger(logger),
)
```

### 5.3 RabbitMQ Options
```go
// 1. Client initialization
client, err := rabbitmq.NewClient(&cfg,
    rabbitmq.WithLogger(logger),
)

// 2. Consumer with functional options
consumer := rabbitmq.NewConsumer(client, "orders-queue",
    rabbitmq.WithConsumerQos(20),
    rabbitmq.WithAutoAck(false),
    rabbitmq.WithExclusive(false),
)

// 3. Publisher with retry options
publisher, err := rabbitmq.NewPublisher(client,
    rabbitmq.WithMaxAttempts(5),
)
```

### 5.4 Kafka Client Options
```go
client, err := kafka.NewClient(&cfg,
    kafka.WithLogger(logger),
    kafka.WithDialer(customDialer),
)
```
