# nvx-go-driver

[![Go Reference](https://pkg.go.dev/badge/github.com/Jkenyut/nvx-go-driver.svg)](https://pkg.go.dev/github.com/Jkenyut/nvx-go-driver)
[![Go Report Card](https://goreportcard.com/badge/github.com/Jkenyut/nvx-go-driver)](https://goreportcard.com/report/github.com/Jkenyut/nvx-go-driver)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Coverage Status](https://img.shields.io/badge/coverage-85%25-brightgreen.svg)](#8-verification-testing-and-health-check)

The `nvx-go-driver` module provides unified client drivers for external infrastructure services. It manages connection pools, automatic reconnection, graceful shutdown, and OpenTelemetry instrumentation for Go applications. The library supports PostgreSQL, Redis, RabbitMQ, and Kafka.

---

## 1. System Architecture and Data Flows

The driver layer manages network connections between application services and backend infrastructure.

### 1.1 System Topology and Component Architecture

```mermaid
flowchart TD
    subgraph APP["Application Layer"]
        ServiceApp["Service Application"]
    end

    subgraph CONFIG["Configuration Layer (Milliseconds)"]
        SQLConf["config.SQLConfig"]
        RedisConf["config.RedisConfig"]
        RabbitConf["config.RabbitMQConfig"]
        KafkaConf["config.KafkaConfig"]
    end

    subgraph DRIVERS["Driver Core Layer"]
        PG["postgres.Client<br/>(pgxpool with Atomic Swap)"]
        RD["redis.Client<br/>(go-redis with Telemetry)"]
        RMQ["rabbitmq.Client<br/>(amqp091-go with Reconnect)"]
        KF["kafka.Client<br/>(kafka-go with Smart Balancer)"]
    end

    subgraph TELEMETRY["Observability Infrastructure"]
        OTel["OpenTelemetry Tracer & Meter"]
        Prom["Prometheus Metrics Collector"]
    end

    subgraph STORAGE["Persistence and Messaging"]
        PostgresDB[("PostgreSQL Database")]
        RedisDB[("Redis Cache")]
        RabbitMQBroker[("RabbitMQ Message Broker")]
        KafkaCluster[("Kafka Event Bus")]
    end

    ServiceApp --> CONFIG
    ServiceApp --> DRIVERS

    CONFIG --> DRIVERS

    DRIVERS -.-> TELEMETRY

    PG -->|"SQL queries (pgx)"| PostgresDB
    RD -->|"Commands (RESP)"| RedisDB
    RMQ -->|"AMQP 0-9-1 protocol"| RabbitMQBroker
    KF -->|"Kafka wire protocol"| KafkaCluster
```

The application initiates driver clients using configuration objects. Drivers expose pool statistics to Prometheus collectors. Drivers record execution latency to OpenTelemetry collectors.

---

### 1.2 Core Transaction Workflow: PostgreSQL Connection and Swap

```mermaid
sequenceDiagram
    autonumber
    participant App as Application Service
    participant Client as postgres.Client
    participant Pool as pgxpool.Pool (Active)
    participant Monitor as Health Monitor Loop
    participant DB as PostgreSQL Server

    App->>Client: Call NewClient(cfg, opts)
    Client->>DB: Dial initial connection pool
    DB-->>Client: Connection successful
    Client->>Client: Store pool reference atomically
    Client-->>App: Return Client pointer

    loop Every HealthCheckPeriod (ms)
        Monitor->>Pool: Send Ping(ctx)
        alt Ping succeeds
            Pool-->>Monitor: Return OK (healthy = true)
        else Ping fails
            Monitor->>Client: Set healthy = false
            Monitor->>DB: Dial new pool with backoff
            DB-->>Monitor: Return new pool connection
            Monitor->>Client: Store new pool atomically
            Monitor->>Pool: Close old pool after five seconds
            Client->>Client: Set healthy = true
        end
    end

    App->>Client: Execute Query(ctx, sql, args)
    Client->>DB: Forward query via active pool
    DB-->>App: Return database rows
```

The health monitor checks connection status periodically. If the ping fails, the monitor creates a replacement connection pool. The driver swaps the pointer atomically to avoid downtime.

---

### 1.3 Core Transaction Workflow: RabbitMQ Publisher and Consumer

```mermaid
sequenceDiagram
    autonumber
    participant App as Application Service
    participant Pub as rabbitmq.Publisher
    participant Broker as RabbitMQ Broker
    participant Sub as rabbitmq.Consumer
    participant Handler as HandlerFunc

    App->>Pub: Call Publish(ctx, exchange, routingKey, msg)
    Pub->>Broker: Send BasicPublish with trace headers
    alt Broker confirms message
        Broker-->>Pub: Deliver Ack(deliveryTag)
        Pub-->>App: Return nil
    else Broker rejects message
        Broker-->>Pub: Deliver Nack(deliveryTag)
        Pub-->>App: Return confirmation error
    end

    Broker->>Sub: Deliver message to queue consumer
    Sub->>Sub: Extract W3C trace context from headers
    Sub->>Handler: Execute handler(ctx, msg)
    alt Handler succeeds
        Handler-->>Sub: Return ActionAck
        Sub->>Broker: Send msg.Ack(false)
    else Handler fails
        Handler-->>Sub: Return ActionNackDiscard
        Sub->>Broker: Send msg.Nack(false, false)
    end
```

The publisher injects W3C trace headers into every message. The consumer extracts trace context before execution. The consumer sends acknowledgments based on handler response.

---

## 2. Data Schema and Lifecycle Operations

The driver defines structured configuration entities and runtime lifecycle states.

### 2.1 Configuration Entity Relationship Model

```mermaid
erDiagram
    CONFIG_LISTENER ||--o{ CONFIG_SQL : references
    CONFIG_LISTENER ||--o{ CONFIG_REDIS : references
    CONFIG_LISTENER ||--o{ CONFIG_RABBITMQ : references
    CONFIG_LISTENER ||--o{ CONFIG_KAFKA : references

    CONFIG_SQL ||--|| POSTGRES_CLIENT : initializes
    CONFIG_REDIS ||--|| REDIS_CLIENT : initializes
    CONFIG_RABBITMQ ||--|| RABBITMQ_CLIENT : initializes
    CONFIG_KAFKA ||--|| KAFKA_CLIENT : initializes

    RABBITMQ_CLIENT ||--o{ RABBITMQ_PUBLISHER : creates
    RABBITMQ_CLIENT ||--o{ RABBITMQ_CONSUMER : creates

    KAFKA_CLIENT ||--o{ KAFKA_WRITER : creates
    KAFKA_CLIENT ||--o{ KAFKA_READER : creates

    CONFIG_SQL {
        string host
        int port
        string username
        string password
        string database
        string schema
        string options
        string connection
        int max_conn
        int min_conn
        int max_conn_lifetime_ms
        int max_conn_idle_time_ms
        int health_check_period_ms
        int connect_timeout_ms
        bool auto_reconnect
        bool enable_telemetry
    }

    CONFIG_REDIS {
        string host
        int port
        string password
        int database
        string connection
        int poolSize
        int minIdleConn
        int maxIdleConn
        int poolTimeoutMs
        int connectTimeoutMs
        int connMaxLifeMs
        bool tls
        bool enableTelemetry
    }

    CONFIG_RABBITMQ {
        string host
        int port
        string username
        string password
        string vhost
        string connection
        bool tls
        int connectTimeoutMs
        int reconnectDurationMs
        int publishTimeoutMs
        bool enableTelemetry
    }

    CONFIG_KAFKA {
        string brokers
        string username
        string password
        string securityProtocol
        string mechanisms
        bool enableTelemetry
    }

    CONFIG_LISTENER {
        string listen
        int port
        string nameService
        string env
    }
```

The configuration entities provide connection parameters for driver instances. All time durations use milliseconds.

---

### 2.2 Lifecycle and State Machine

```mermaid
stateDiagram-v2
    [*] --> Uninitialized

    Uninitialized --> Connecting: Call NewClient()
    
    state Connecting {
        [*] --> Dialing
        Dialing --> Handshake: Socket open
        Handshake --> Authenticated: Credentials valid
        Handshake --> ConnectionFailed: Handshake error
        ConnectionFailed --> Dialing: Retry with backoff
    }

    Connecting --> Connected: Initial ping succeeds
    Connecting --> Closed: Retry limit exceeded

    state Connected {
        [*] --> Idle
        Idle --> Processing: Query or message received
        Processing --> Idle: Operation completes
    }

    Connected --> Reconnecting: Connection drops or ping fails
    
    state Reconnecting {
        [*] --> BackoffWait
        BackoffWait --> ReDialing: Timer expires
        ReDialing --> SwappingPool: New connection ready
        ReDialing --> BackoffWait: Reconnect fails
        SwappingPool --> [*]
    }

    Reconnecting --> Connected: Pool swapped
    Connected --> Draining: Call Close()
    Reconnecting --> Draining: Call Close()

    state Draining {
        [*] --> StopIngress
        StopIngress --> WaitForInflight: Wait for active connections
        WaitForInflight --> ForceClose: 30s timeout expires
        WaitForInflight --> CleanShutdown: Active count reaches zero
    }

    Draining --> Closed: Resources released
    Closed --> [*]
```

The state machine manages connection states from startup to shutdown. Drivers handle disconnections automatically through exponential backoff.

---

## 3. Prerequisites and Tooling

Install the verified tools listed in the table below:

| Tool | Supported Version | Functional Purpose |
| :--- | :--- | :--- |
| Go Compiler | 1.22+ / 1.27+ | Compiles the driver library and test suites. |
| PostgreSQL | 14.0+ | Serves relational database connections via pgx. |
| Redis Server | 6.2+ / 7.0+ | Serves in-memory caching and session commands. |
| RabbitMQ | 3.10+ | Serves message broker queues and exchange routing. |
| Apache Kafka | 3.0+ | Serves event streaming topic partitions. |
| Docker Engine | 24.0+ | Executes local container infrastructure. |

---

## 4. Directory Structure

The repository maintains modular separation across configuration and driver packages:

```text
.
├── .github/                # Stores GitHub Actions continuous integration workflows.
│   └── workflows/
│       └── test.yml        # Executes automated unit test and lint pipelines.
├── config/                 # Implements configuration loading and default schemas.
│   ├── config.go           # Declares SQL, Redis, RabbitMQ, and Kafka config structs.
│   ├── generate.go         # Provides YAML config loading with reflection defaults.
│   ├── generate_test.go    # Tests config reflection and environment parsing.
│   ├── kafka_config.go     # Applies default settings for Kafka clients.
│   ├── listener_config.go  # Normalizes listener service names and environments.
│   ├── rabbitmq_config.go  # Applies default settings for RabbitMQ clients.
│   ├── redis_config.go     # Applies default settings for Redis clients.
│   └── sql_config.go       # Applies CPU-aware pool defaults for PostgreSQL.
├── kafka/                  # Implements Kafka writer and reader wrappers.
│   ├── kafka.go            # Implements Kafka client, balancer, and trace carrier.
│   └── kafka_test.go       # Tests Kafka configuration and protocol validation.
├── postgres/               # Implements zero-downtime PostgreSQL client pool.
│   ├── client.go           # Manages pgxpool lifecycle, metrics, and transactions.
│   ├── client_test.go      # Tests connection options and DSN construction.
│   ├── tracer.go           # Implements OpenTelemetry query tracing and metrics.
│   └── tracer_test.go      # Tests SQL keyword extraction and comment parsing.
├── rabbitmq/               # Implements AMQP message publisher and consumer.
│   ├── consumer.go         # Manages queue consumer loop and panic recovery.
│   ├── publisher.go        # Manages message publication with broker confirms.
│   ├── rabbitmq.go         # Manages AMQP connection with reconnect loop.
│   └── rabbitmq_test.go    # Tests AMQP client configuration and masking.
├── redis/                  # Implements Redis client and pool metrics.
│   ├── redis.go            # Manages go-redis client with JSON helpers.
│   └── redis_test.go       # Tests Redis option builders and connection logic.
├── ARCHITECTURE.md         # Explains architectural design and pool swapping.
├── CONTRIBUTING.md         # Explains contribution rules and style guides.
├── LICENSE                 # Contains software license terms.
├── Makefile                # Defines build, test, and lint targets.
├── README.md               # Contains primary technical documentation.
├── go.mod                  # Declares module dependencies and Go version.
└── go.sum                  # Contains cryptographic checksums of dependencies.
```

---

## 5. Configuration Parameters

Configure drivers through YAML configuration files or structured code options:

### 5.1 PostgreSQL Configuration (`config.SQLConfig`)

| YAML Key | Type | Default | Description (ASD-STE100) |
| :--- | :--- | :--- | :--- |
| `enable` | boolean | `true` | Enables the PostgreSQL database client. |
| `host` | string | `0.0.0.0` | Sets the PostgreSQL server host address. |
| `port` | integer | `5432` | Sets the PostgreSQL server port. |
| `username` | string | `postgres` | Sets the database login username. |
| `password` | string | `postgres` | Sets the database login password. |
| `database` | string | `postgres` | Sets the target database name. |
| `schema` | string | `public` | Sets the PostgreSQL search path schema. |
| `options` | string | `sslmode=disable` | Sets additional connection parameters. |
| `connection` | string | `""` | Overrides parameters with full connection DSN. |
| `max_conn` | integer | `CPU * 8` | Sets maximum total database connections. |
| `min_conn` | integer | `max(4, CPU)` | Sets minimum standby database connections. |
| `max_conn_lifetime_ms` | integer | `3600000` | Sets maximum connection lifetime in milliseconds. |
| `max_conn_idle_time_ms` | integer | `600000` | Sets connection idle timeout in milliseconds. |
| `health_check_period_ms`| integer | `15000` | Sets health check interval in milliseconds. |
| `connect_timeout_ms` | integer | `10000` | Sets dial timeout limit in milliseconds. |
| `auto_reconnect` | boolean | `true` | Enables automatic pool reconnection on error. |
| `enable_telemetry` | boolean | `false` | Enables OpenTelemetry SQL query tracing. |

### 5.2 Redis Configuration (`config.RedisConfig`)

| YAML Key | Type | Default | Description (ASD-STE100) |
| :--- | :--- | :--- | :--- |
| `enable` | boolean | `false` | Enables the Redis client connection. |
| `host` | string | `127.0.0.1` | Sets the Redis server host address. |
| `port` | integer | `6379` | Sets the Redis server port. |
| `password` | string | `""` | Sets the Redis authentication password. |
| `database` | integer | `0` | Sets the target Redis database index. |
| `poolSize` | integer | `10` | Sets maximum connections in the Redis pool. |
| `minIdleConn` | integer | `5` | Sets minimum idle connections in the pool. |
| `maxIdleConn` | integer | `15` | Sets maximum idle connections in the pool. |
| `poolTimeoutMs` | integer | `30000` | Sets pool acquisition timeout in milliseconds. |
| `connectTimeoutMs` | integer | `5000` | Sets connection dial timeout in milliseconds. |
| `connMaxLifeMs` | integer | `600000` | Sets connection maximum lifetime in milliseconds. |
| `tls` | boolean | `false` | Enables TLS transport encryption. |
| `enableTelemetry` | boolean | `false` | Enables OpenTelemetry Redis instrumentation. |

### 5.3 RabbitMQ Configuration (`config.RabbitMQConfig`)

| YAML Key | Type | Default | Description (ASD-STE100) |
| :--- | :--- | :--- | :--- |
| `enable` | boolean | `false` | Enables the RabbitMQ client connection. |
| `host` | string | `127.0.0.1` | Sets the RabbitMQ broker host address. |
| `port` | integer | `5672` | Sets the RabbitMQ broker port. |
| `username` | string | `guest` | Sets the RabbitMQ authentication username. |
| `password` | string | `guest` | Sets the RabbitMQ authentication password. |
| `vhost` | string | `/` | Sets the RabbitMQ virtual host path. |
| `tls` | boolean | `false` | Enables AMQPS transport encryption. |
| `connectTimeoutMs` | integer | `10000` | Sets connection handshake timeout in milliseconds. |
| `reconnectDurationMs` | integer | `5000` | Sets reconnect retry interval in milliseconds. |
| `publishTimeoutMs` | integer | `5000` | Sets publish confirmation timeout in milliseconds. |
| `enableTelemetry` | boolean | `false` | Enables W3C distributed trace propagation. |

### 5.4 Kafka Configuration (`config.KafkaConfig`)

| YAML Key | Type | Default | Description (ASD-STE100) |
| :--- | :--- | :--- | :--- |
| `enable` | boolean | `false` | Enables the Kafka client connection. |
| `brokers` | string | `127.0.0.1:9092` | Sets comma-separated Kafka broker addresses. |
| `username` | string | `""` | Sets SASL authentication username. |
| `password` | string | `""` | Sets SASL authentication password. |
| `securityProtocol` | string | `PLAINTEXT` | Sets transport security protocol. |
| `mechanisms` | string | `PLAIN` | Sets SASL authentication mechanism. |
| `enableTelemetry` | boolean | `false` | Enables OpenTelemetry producer tracing. |

> [!NOTE]
> All time parameters use milliseconds to prevent configuration ambiguity across container environments.

---

## 6. Installation and Execution Procedure

Follow these procedural steps to install, configure, and execute the drivers.

### Step 1: Install Package Dependency

Add the driver module to your Go project:

```bash
go get github.com/Jkenyut/nvx-go-driver
```

---

### Step 2: Initialize PostgreSQL Driver

Create a PostgreSQL client with connection pooling and telemetry:

```go
package main

import (
	"context"
	"log"

	"github.com/Jkenyut/nvx-go-driver/config"
	"github.com/Jkenyut/nvx-go-driver/postgres"
)

func main() {
	cfg := config.SQLConfig{
		Connection:      "postgres://user:password@localhost:5432/mydb?sslmode=disable",
		EnableTelemetry: true,
	}

	client, err := postgres.NewClient(&cfg)
	if err != nil {
		log.Fatalf("database connection failed: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	rows, err := client.Pool().Query(ctx, "SELECT id, name FROM users")
	if err != nil {
		log.Fatalf("query execution failed: %v", err)
	}
	defer rows.Close()
}
```

---

### Step 3: Initialize RabbitMQ Publisher and Consumer

Publish and consume messages with W3C trace context propagation:

```go
package main

import (
	"context"
	"log"

	"github.com/Jkenyut/nvx-go-driver/config"
	"github.com/Jkenyut/nvx-go-driver/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	cfg := config.RabbitMQConfig{
		Host:            "127.0.0.1",
		Port:            5672,
		EnableTelemetry: true,
	}

	client, err := rabbitmq.NewClient(&cfg)
	if err != nil {
		log.Fatalf("rabbitmq connection failed: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// 1. Publisher
	publisher, err := rabbitmq.NewPublisher(client)
	if err != nil {
		log.Fatalf("publisher creation failed: %v", err)
	}
	defer publisher.Close()

	msg := &amqp.Publishing{
		ContentType: "application/json",
		Body:        []byte(`{"order_id": "1001"}`),
	}
	err = publisher.Publish(ctx, "orders", "order.created", msg, false, false)
	if err != nil {
		log.Fatalf("publish message failed: %v", err)
	}

	// 2. Consumer
	consumer := rabbitmq.NewConsumer(client, "orders-queue")
	consumer.Start(ctx, func(ctx context.Context, delivery amqp.Delivery) rabbitmq.Action {
		log.Printf("Received delivery body: %s", string(delivery.Body))
		return rabbitmq.ActionAck
	})
	defer consumer.Close()
}
```

---

### Step 4: Initialize Redis Client

Execute commands and JSON operations with Redis:

```go
package main

import (
	"context"
	"log"
	"time"

	"github.com/Jkenyut/nvx-go-driver/config"
	"github.com/Jkenyut/nvx-go-driver/redis"
)

func main() {
	cfg := config.RedisConfig{
		Host:            "127.0.0.1",
		Port:            6379,
		EnableTelemetry: true,
	}

	client, err := redis.NewClient(&cfg)
	if err != nil {
		log.Fatalf("redis connection failed: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	err = client.Set(ctx, "session:1001", "active", 10*time.Minute)
	if err != nil {
		log.Fatalf("redis set failed: %v", err)
	}

	val, err := client.Get(ctx, "session:1001")
	if err != nil {
		log.Fatalf("redis get failed: %v", err)
	}
	log.Printf("session value: %s", val)
}
```

---

### Step 5: Initialize Kafka Client

Publish messages using partition-aware hashing:

```go
package main

import (
	"context"
	"log"

	"github.com/Jkenyut/nvx-go-driver/config"
	"github.com/Jkenyut/nvx-go-driver/kafka"
)

func main() {
	cfg := config.KafkaConfig{
		Brokers:         "127.0.0.1:9092",
		EnableTelemetry: true,
	}

	client, err := kafka.NewClient(&cfg)
	if err != nil {
		log.Fatalf("kafka connection failed: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	err = client.Publish(ctx, "orders", []byte("key-1001"), []byte(`{"status":"created"}`))
	if err != nil {
		log.Fatalf("kafka publish failed: %v", err)
	}
}
```

---

## 7. API and Operational Interface

The table summarizes primary driver functions and methods across core modules:

| Function Interface | Operational Module | Functional Role (ASD-STE100) |
| :--- | :--- | :--- |
| `postgres.NewClient(cfg, opts...)` | PostgreSQL | Creates a PostgreSQL client with atomic pool management. |
| `client.Pool()` | PostgreSQL | Returns the active `*pgxpool.Pool` reference. |
| `client.Query(ctx, sql, args...)` | PostgreSQL | Executes a SQL query and returns rows. |
| `client.Exec(ctx, sql, args...)` | PostgreSQL | Executes a SQL command and returns affected row counts. |
| `client.RunInTx(ctx, fn)` | PostgreSQL | Executes a function inside a transaction with auto-rollback. |
| `client.Metrics()` | PostgreSQL | Returns Prometheus pool and connection metric functions. |
| `client.Close()` | PostgreSQL | Drains active connections and closes the pool. |
| `redis.NewClient(cfg, opts...)` | Redis | Creates a Redis client with auto-reconnect and tracing. |
| `client.Set(ctx, key, val, exp)` | Redis | Sets a key value with an expiration duration. |
| `client.Get(ctx, key)` | Redis | Retrieves the string value of a key. |
| `client.SetJSON(ctx, key, val, exp)`| Redis | Marshals data to JSON and stores it in Redis. |
| `client.GetJSON(ctx, key, dest)` | Redis | Retrieves JSON data and unmarshals it into a destination pointer. |
| `client.Metrics()` | Redis | Returns Prometheus cache hits, misses, and pool metrics. |
| `rabbitmq.NewClient(cfg, opts...)` | RabbitMQ | Creates an AMQP client with background reconnect monitoring. |
| `rabbitmq.NewPublisher(client, opts)`| RabbitMQ | Creates a publisher with broker confirmation support. |
| `publisher.Publish(ctx, ex, rk, msg)`| RabbitMQ | Publishes a message and waits for broker confirmation. |
| `rabbitmq.NewConsumer(client, q, opts)`| RabbitMQ | Creates a consumer with QoS prefetch and auto-ack options. |
| `consumer.Start(ctx, handler)` | RabbitMQ | Starts asynchronous queue consumption with panic recovery. |
| `kafka.NewClient(cfg, opts...)` | Kafka | Creates a Kafka factory client with broker dialer validation. |
| `client.Publish(ctx, topic, key, val)`| Kafka | Publishes a message with smart Murmur2 partition balancing. |
| `client.NewReader(topic, groupID)` | Kafka | Returns a reader for consumer group message processing. |
| `client.NewWriter()` | Kafka | Returns a writer with batching and retry transports. |

---

## 8. Verification, Testing and Health Check

Run verified commands to validate driver correctness, performance, and formatting.

### Step 1: Run Unit Tests with Race Detector

Execute the unit test suite across all driver packages:

```bash
go test -v -race ./...
```

Expected terminal log:

```text
=== RUN   TestSetDefaults_BasicStruct
--- PASS: TestSetDefaults_BasicStruct (0.00s)
=== RUN   TestLoad_CreatesFileIfMissing
--- PASS: TestLoad_CreatesFileIfMissing (0.00s)
=== RUN   TestSQLConfig_WithDefaults
--- PASS: TestSQLConfig_WithDefaults (0.00s)
PASS
ok  	github.com/Jkenyut/nvx-go-driver/config	1.429s
=== RUN   TestWithDefaults
--- PASS: TestWithDefaults (0.00s)
PASS
ok  	github.com/Jkenyut/nvx-go-driver/kafka	1.679s
=== RUN   TestBuildDSN
--- PASS: TestBuildDSN (0.00s)
=== RUN   TestExtractSQLOperation
--- PASS: TestExtractSQLOperation (0.00s)
PASS
ok  	github.com/Jkenyut/nvx-go-driver/postgres	2.304s
=== RUN   TestWithDefaults
--- PASS: TestWithDefaults (0.00s)
=== RUN   TestConsumer_SettersGuardWhenStarted
--- PASS: TestConsumer_SettersGuardWhenStarted (0.00s)
PASS
ok  	github.com/Jkenyut/nvx-go-driver/rabbitmq	2.930s
=== RUN   TestBuildRedisOptions
--- PASS: TestBuildRedisOptions (0.00s)
PASS
ok  	github.com/Jkenyut/nvx-go-driver/redis	2.402s
```

---

### Step 2: Run Static Code Analysis

Execute static analysis checks:

```bash
go vet ./...
```

> [!CAUTION]
> Fix all reported diagnostics before you push code to remote branches.

---

## 9. Troubleshooting and Fault Isolation

Review the table below to resolve common configuration and runtime errors:

| Observed Error | Root Cause (ASD-STE100) | Remediation Procedure (ASD-STE100) |
| :--- | :--- | :--- |
| `database connection failed` | The PostgreSQL host or port is unreachable. | Check network connectivity to the database port. |
| `database disabled in config` | `SQLConfig.Enable` is set to false. | Set `enable: true` in your configuration file. |
| `publisher channel unavailable` | The RabbitMQ connection dropped unexpectedly. | Wait for reconnect loop or check broker status. |
| `publisher publish timeout` | The broker did not acknowledge publication. | Check broker queue memory limits and cluster load. |
| `all kafka brokers unreachable` | The Kafka bootstrap broker is offline. | Check broker IP addresses in `KafkaConfig.Brokers`. |
| `redis connection failed` | The Redis host is offline or password failed. | Check Redis server credentials and port status. |
| `read-only file system` error | `LoadInto` attempted disk write in container. | Mount configuration files with write permissions or use defaults. |

> [!WARNING]
> Always defer the `Close()` function on initialized clients. Unclosed drivers leak socket descriptors and background goroutines.

---

## 10. License

This software is licensed under the Apache License 2.0. Refer to [LICENSE](LICENSE) for full legal terms.
