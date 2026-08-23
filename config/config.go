package config

// Listener default configuration
type Listener struct {
	// Listen host/interface address
	// Example:
	// 0.0.0.0 = listen on all interfaces
	Listen string `yaml:"listen" default:"0.0.0.0"`

	// Port application listening port
	Port int `yaml:"port" default:"8081"`

	// NameService application/service name
	NameService string `yaml:"nameService" default:"service-name"`

	// Env application environment
	// Example:
	// development
	// staging
	// production
	Env string `yaml:"env" default:"development"`
}

// SQLConfig holds the SQL configuration.
type SQLConfig struct {
	// Enable enables/disables SQL database connection
	Enable bool `yaml:"enable" json:"enable" default:"true"`

	// EnableTelemetry enables tracing/metrics
	EnableTelemetry bool `yaml:"enable_telemetry" json:"enable_telemetry" default:"false"`

	// Host database host/address
	Host string `yaml:"host" json:"host" default:"0.0.0.0"`

	// Port database port
	Port int `yaml:"port" json:"port" default:"5432"`

	// Username database username
	Username string `yaml:"username" json:"username" default:"postgres"`

	// Password database password
	Password string `yaml:"password" json:"password" default:"postgres"`

	// Database database/schema name
	Database string `yaml:"database" json:"database" default:"postgres"`

	// Schema sets the PostgreSQL search_path for this connection.
	// Controls which schema is searched first when unqualified table names are used.
	// Defaults to "public" if empty.
	Schema string `yaml:"schema" json:"schema" default:"public"`

	// Options additional database connection options
	// Example:
	// sslmode=disable
	Options string `yaml:"options" json:"options" default:"sslmode=disable"`

	// Connection full connection string override
	// If provided, individual connection fields may be ignored
	Connection string `yaml:"connection" json:"connection" default:""`

	// MaxConn maximum total active database connections
	// Prevents database overload from excessive concurrent connections
	MaxConn int `yaml:"max_conn" json:"max_conn" default:"0"`

	// MinConn minimum standby connections maintained in the pool
	// Helps reduce latency by keeping reusable connections ready
	MinConn int `yaml:"min_conn" json:"min_conn" default:"0"`

	// MaxConnLifetime maximum lifetime of a connection in seconds
	// Old connections will be recycled automatically
	//
	// Useful for:
	// - load balancer rotation
	// - stale connection prevention
	// - failover handling
	MaxConnLifetime int `yaml:"max_conn_lifetime_seconds" json:"max_conn_lifetime_seconds" default:"3600"`

	// MaxConnIdleTime maximum idle duration before connection is closed
	// Helps release unused resources
	MaxConnIdleTime int `yaml:"max_conn_idle_time_seconds" json:"max_conn_idle_time_seconds" default:"600"`

	// HealthCheckPeriod interval for connection health checking
	// Unhealthy/stale connections will be replaced automatically
	HealthCheckPeriod int `yaml:"health_check_period_seconds" json:"health_check_period_seconds" default:"15"`

	// ConnectTimeout maximum time to establish database connection
	// Prevents hanging connection attempts
	ConnectTimeout int `yaml:"connect_timeout_seconds" json:"connect_timeout_seconds" default:"10"`

	// AutoReconnect automatically reconnects on connection failure
	AutoReconnect bool `yaml:"auto_reconnect" json:"auto_reconnect" default:"true"`

	// ApplicationName identifies this service in pg_stat_activity.
	// Defaults to "nvx-go-driver" if empty.
	ApplicationName string `yaml:"application_name" json:"application_name" default:""`

	// UseMock enables mock database implementation
	// Useful for testing and local development
	UseMock bool `yaml:"use_mock" json:"use_mock" default:"false"`

	// QueryExecMode configures pgx query execution mode.
	// Use "exec" or "simple_protocol" for pgBouncer transaction mode.
	QueryExecMode string `yaml:"query_exec_mode" json:"query_exec_mode" default:"cache_statement"`
}

// RabbitMQConfig holds the RabbitMQ configuration.
type RabbitMQConfig struct {
	// Enable enables/disables RabbitMQ connection
	Enable bool `yaml:"enable" default:"false" desc:"config:rabbitmq:enable"`

	// EnableTelemetry enables tracing/metrics
	EnableTelemetry bool `yaml:"enableTelemetry" default:"false" desc:"config:rabbitmq:enableTelemetry"`

	// Connection full RabbitMQ connection URL override
	// Example: amqps://user:password@localhost:5672/vhost
	Connection string `yaml:"connection" default:"" desc:"config:rabbitmq:connection"`

	// VHost RabbitMQ virtual host
	VHost string `yaml:"vhost" default:"/" desc:"config:rabbitmq:vhost"`

	// Host RabbitMQ server host/address
	Host string `yaml:"host" default:"0.0.0.0" desc:"config:rabbitmq:host"`

	// Port RabbitMQ server port
	Port int `yaml:"port" default:"5672" desc:"config:rabbitmq:port"`

	// Username RabbitMQ username
	Username string `yaml:"username" default:"guest" desc:"config:rabbitmq:username"`

	// Password RabbitMQ password
	Password string `yaml:"password" default:"guest" desc:"config:rabbitmq:password"`

	// TLS enables amqps:// transport encryption.
	TLS bool `yaml:"tls" default:"false" desc:"config:rabbitmq:tls"`

	// InsecureSkipVerify skips TLS certificate verification.
	// Not recommended for production environments.
	InsecureSkipVerify bool `yaml:"insecureSkipVerify" default:"false" desc:"config:rabbitmq:insecureSkipVerify"`

	// ReconnectDuration retry interval before reconnect attempt in seconds
	ReconnectDuration int `yaml:"reconnectDuration" default:"5" desc:"config:rabbitmq:reconnectDuration"`

	// ConnectTimeout maximum time for TCP/TLS/AMQP handshake in seconds
	ConnectTimeout int `yaml:"connectTimeout" default:"10" desc:"config:rabbitmq:connectTimeout"`

	// PublishTimeout maximum time for one publish attempt and broker confirm in seconds
	PublishTimeout int `yaml:"publishTimeout" default:"5" desc:"config:rabbitmq:publishTimeout"`

	// DedicatedConnection enables dedicated connection per producer/consumer
	// Helps isolate workload and improve stability
	DedicatedConnection bool `yaml:"dedicatedConnection" default:"false" desc:"config:rabbitmq:dedicatedConnection"`

	// UseMock enables mock RabbitMQ implementation
	UseMock bool `yaml:"useMock" default:"false" desc:"config:useMock"`
}

// RedisConfig holds the Redis configuration.
type RedisConfig struct {
	// Enable enables/disables Redis connection
	Enable bool `yaml:"enable" default:"false" desc:"config:redis:enable"`

	// EnableTelemetry enables tracing/metrics
	EnableTelemetry bool `yaml:"enableTelemetry" default:"false" desc:"config:redis:enableTelemetry"`

	// Database Redis database index
	Database int `yaml:"database" default:"0" desc:"config:redis:database"`

	// Connection full Redis connection URL override
	// Example: redis://user:password@localhost:6379/0
	Connection string `yaml:"connection" default:"" desc:"config:redis:connection"`

	// ApplicationName identifies this service in Redis CLIENT LIST.
	// Defaults to "nvx-go-driver" if empty.
	ApplicationName string `yaml:"applicationName" default:"" desc:"config:redis:applicationName"`

	// Host Redis server host/address
	Host string `yaml:"host" default:"0.0.0.0" desc:"config:redis:host"`

	// Port Redis server port
	Port int `yaml:"port" default:"6379" desc:"config:redis:port"`

	// Password Redis authentication password
	Password string `yaml:"password" default:"" desc:"config:redis:password"`

	// TLS enables TLS transport encryption.
	TLS bool `yaml:"tls" default:"false" desc:"config:redis:tls"`

	// InsecureSkipVerify skips TLS certificate verification.
	// Not recommended for production environments.
	InsecureSkipVerify bool `yaml:"insecureSkipVerify" default:"false" desc:"config:redis:insecureSkipVerify"`

	// Pool deprecated/basic pool configuration
	Pool int `yaml:"pool" default:"10" desc:"config:redis:pool"`

	// AutoReconnect automatically reconnects when connection is lost
	AutoReconnect bool `yaml:"autoReconnect" default:"false" desc:"config:redis:autoReconnect"`

	// StartInterval reconnect retry interval in seconds
	StartInterval int `yaml:"startInterval" default:"2" desc:"config:redis:startInterval"`

	// MaxError maximum allowed reconnect errors before stopping retries
	MaxError int `yaml:"maxError" default:"5" desc:"config:redis:maxError"`

	// PoolSize maximum total Redis connections in pool
	PoolSize int `yaml:"poolSize" default:"30" desc:"config:redis:poolSize"`

	// PoolTimeout maximum wait time for acquiring connection from pool
	PoolTimeout int `yaml:"poolTimeout" default:"30" desc:"config:redis:poolTimeout"`

	// ConnectTimeout maximum time to establish Redis connection
	ConnectTimeout int `yaml:"connectTimeout" default:"5" desc:"config:redis:connectTimeout"`

	// MinIdleConn minimum idle connections maintained
	// Helps improve performance by keeping warm connections
	MinIdleConn int `yaml:"minIdleConn" default:"7" desc:"config:redis:minIdleConn"`

	// MaxIdleConn maximum idle connections allowed
	// Excess idle connections may be closed automatically
	MaxIdleConn int `yaml:"maxIdleConn" default:"15" desc:"config:redis:maxIdleConn"`

	// ConnMaxLife maximum lifetime of Redis connection in seconds
	// Old connections will be recycled automatically
	ConnMaxLife int `yaml:"connMaxLife" default:"600" desc:"config:redis:connMaxLife"`

	// UseMock enables mock Redis implementation
	UseMock bool `yaml:"useMock" default:"false" desc:"config:useMock"`
}

// KafkaConfig holds the Kafka configuration.
type KafkaConfig struct {
	// Enable enables/disables Kafka connection
	Enable bool `yaml:"enable" default:"false" desc:"config:kafka:enable"`

	// EnableTelemetry enables tracing/metrics
	EnableTelemetry bool `yaml:"enableTelemetry" default:"false" desc:"config:kafka:enableTelemetry"`

	// Brokers Kafka broker addresses (comma-separated)
	// Example:
	// broker1:9092,broker2:9092
	Brokers string `yaml:"brokers" default:"0.0.0.0:9092" desc:"config:kafka:brokers"`

	// Registry schema registry address
	// Used for Avro/Schema-based serialization
	Registry string `yaml:"registry" default:"" desc:"config:kafka:registry"`

	// Username Kafka authentication username
	Username string `yaml:"username" default:"" desc:"config:kafka:username"`

	// Password Kafka authentication password
	Password string `yaml:"password" default:"" desc:"config:kafka:password"`

	// SecurityProtocol Kafka security protocol
	// Example:
	// SASL_SSL
	// PLAINTEXT
	SecurityProtocol string `yaml:"securityProtocol" default:"SASL_SSL" desc:"config:kafka:securityProtocol"`

	// Mechanisms SASL authentication mechanism
	// Example:
	// PLAIN
	// SCRAM-SHA-256
	Mechanisms string `yaml:"mechanisms" default:"PLAIN" desc:"config:kafka:mechanisms"`

	// UseMock enables mock Kafka implementation
	UseMock bool `yaml:"useMock" default:"false" desc:"config:useMock"`

	// InsecureSkipVerify skips SSL certificate verification
	// Not recommended for production environments
	InsecureSkipVerify bool `yaml:"insecureSkipVerify" default:"false" desc:"config:kafka:insecureSkipVerify"`

	// Debug enables Kafka client debug mode
	// Example:
	// consumer
	// producer
	// broker
	Debug string `yaml:"debug" default:"consumer" desc:"config:kafka:debug"`
}
