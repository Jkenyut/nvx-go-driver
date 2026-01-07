# nvx-go-driver

![Go Version](https://img.shields.io/badge/go-1.21%2B-blue)
![License](https://img.shields.io/badge/license-private-red)

**nvx-go-driver** is a production-ready PostgreSQL driver wrapper for Go, built on top of [pgx/v5](https://github.com/jackc/pgx). It is designed for high-availability microservices, providing zero-downtime pool switching, structured logging, and built-in metrics.

## Features

- **Zero-Downtime Reconnections**: Automatically handles database restarts or network failures by swapping connection pools transparently.
- **Graceful Shutdown**: Waits for active queries to drain before closing connections.
- **Observability**: Built-in Prometheus-compatible metrics (`PoolHealth`, `ReconnectsTotal`, etc.).
- **Structured Logging**: Integrated with `zerolog` for clear, leveled logs.
- **Smart Defaults**: CPU-aware configuration for pool sizes.

## Installation

```bash
go get github.com/Jkenyut/nvx-go-driver
```

## Quick Start

```go
package main

import (
	"context"
	"os"
	"time"

	"github.com/Jkenyut/nvx-go-driver/config"
	"github.com/Jkenyut/nvx-go-driver/database"
	"github.com/rs/zerolog"
)

func main() {
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	// 1. Configure the database
	cfg := config.SQLConfig{
		Enable:   true,
		Host:     "localhost",
		Port:     5432,
		Username: "postgres",
		Password: "password",
		Database: "mydb",
		// Auto-calculate pool size based on CPU if set to 0
		MaxConn: 0,
	}

	// 2. Initialize the client
	dbClient, err := database.NewPGXClient(cfg, &logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to connect to database")
	}
	defer dbClient.Close()

	// 3. Connect to the pool
	pool := dbClient.Pool()
	
	// 4. Run simple query
	var now time.Time
	err = pool.QueryRow(context.Background(), "SELECT NOW()").Scan(&now)
	if err != nil {
		logger.Error().Err(err).Msg("Query failed")
		return
	}
	
	logger.Info().Time("now", now).Msg("Database query successful")
}
```

## Configuration (`SQLConfig`)

| Field | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `Enable` | `bool` | `true` | Enable/disable the database client. |
| `Host` | `string` | - | Database hostname. |
| `Port` | `int` | `5432` | Database port. |
| `Username` | `string` | - | Database username. |
| `Password` | `string` | - | Database password. |
| `Database` | `string` | - | Database name. |
| `MaxConn` | `int` | `CPU * 8` | Maximum number of connections. |
| `MinConn` | `int` | `Min(4, CPU)` | Minimum number of idle connections. |
| `AutoReconnect` | `bool` | `true` | Enable background monitor to reconnect on failure. |

## Observability

The client exposes a `Metrics()` method that returns a struct of functions suitable for Prometheus collectors.

```go
metrics := dbClient.Metrics()

// Example: Registering with a metrics provider
// Gauge.Set(metrics.PoolHealth())
// Counter.Add(metrics.ReconnectsTotal())
```
