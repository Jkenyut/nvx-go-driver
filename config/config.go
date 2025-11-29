package config

// Listener default config
type Listener struct {
	Listen string `yaml:"listen" default:"0.0.0.0"`
	Port   int    `yaml:"port" default:"8081"`
}

type SQLConfig struct {
	Enable   bool   `yaml:"enable" default:"false"`
	Host     string `yaml:"host" default:"127.0.0.1"`
	Port     int    `yaml:"port" default:"5432"`
	Username string `yaml:"username" default:"postgres"`
	Password string `yaml:"password" default:"postgres"`
	Database string `yaml:"database" default:"postgres"`
	Options  string `yaml:"options" default:"sslmode=disable"`

	// Optional: override full DSN
	Connection string `yaml:"connection" default:""`

	// Behavior
	AutoReconnect bool `yaml:"autoReconnect" default:"true"`

	// PGX Pool defaults (production best practice)
	CustomPool        bool `yaml:"customPool" default:"false"`
	MaxConn           int  `yaml:"maxConn" default:"20"`         // best for most microservices
	MinConn           int  `yaml:"minConn" default:"2"`          // keeps warm
	MaxConnIdleTime   int  `yaml:"maxConnIdleTime" default:"60"` // seconds
	LifeTime          int  `yaml:"lifeTime" default:"30"`        // minutes
	HealthCheckPeriod int  `yaml:"healthCheck" default:"30"`     // seconds

	// Timeout
	ConnectTimeout int `yaml:"connectTimeout" default:"5"` // seconds

	// Disable prepared stmt (rare)
	SimpleProtocol bool `yaml:"simpleProtocol" default:"false"`

	// Retry
	StartInterval int  `yaml:"startInterval" default:"2"`
	MaxError      int  `yaml:"maxError" default:"5"`
	UseMock       bool `yaml:"useMock" default:"false"  desc:"config:useMock"`
}

type RabbitMQConfig struct {
	Enable              bool   `yaml:"enable" default:"false" desc:"config:rabbitmq:enable"`
	Host                string `yaml:"host" default:"127.0.0.1" desc:"config:rabbitmq:host"`
	Port                int    `yaml:"port" default:"5672" desc:"config:rabbitmq:port"`
	Username            string `yaml:"username" default:"guest"  desc:"config:rabbitmq:username"`
	Password            string `yaml:"password" default:"guest" desc:"config:rabbitmq:password"`
	ReconnectDuration   int    `yaml:"reconnectDuration" default:"5" desc:"config:rabbitmq:reconnectDuration"`
	DedicatedConnection bool   `yaml:"dedicatedConnection" default:"false" desc:"config:rabbitmq:dedicatedConnection"`
	UseMock             bool   `yaml:"useMock" default:"false"  desc:"config:useMock"`
}

type RedisConfig struct {
	Enable        bool   `yaml:"enable" default:"false" desc:"config:redis:enable"`
	Host          string `yaml:"host" default:"127.0.0.1" desc:"config:redis:host"`
	Port          int    `yaml:"port" default:"6379" desc:"config:redis:port"`
	Password      string `yaml:"password" default:"" desc:"config:redis:password"`
	Pool          int    `yaml:"pool" default:"10" desc:"config:redis:pool"`
	AutoReconnect bool   `yaml:"autoReconnect" default:"false"  desc:"config:redis:autoReconnect"`
	StartInterval int    `yaml:"startInterval" default:"2"  desc:"config:redis:startInterval"`
	MaxError      int    `yaml:"maxError" default:"5"  desc:"config:redis:maxError"`
	PoolSize      int    `yaml:"poolSize" default:"30" desc:"config:redis:poolSize"`
	PoolTimeout   int    `yaml:"poolTimeout" default:"30" desc:"config:redis:poolTimeout"`
	MinIdleConn   int    `yaml:"minIdleConn" default:"7" desc:"config:redis:minIdleConn"`
	MaxIdleConn   int    `yaml:"maxIdleConn" default:"15" desc:"config:redis:maxIdleConn"`
	ConnMaxLife   int    `yaml:"connMaxLife" default:"600" desc:"config:redis:connMaxLife"`
	UseMock       bool   `yaml:"useMock" default:"false"  desc:"config:useMock"`
}

type KafkaConfig struct {
	Enable           bool   `yaml:"enable" default:"false" desc:"config:kafka:enable"`
	Host             string `yaml:"host" default:"127.0.0.1:9092" desc:"config:kafka:host"`
	Registry         string `yaml:"registry" default:"" desc:"config:kafka:registry"`
	Username         string `yaml:"username" default:""  desc:"config:kafka:username"`
	Password         string `yaml:"password" default:"" desc:"config:kafka:password"`
	SecurityProtocol string `yaml:"securityProtocol" default:"SASL_SSL"  desc:"config:kafka:securityProtocol"`
	Mechanisms       string `yaml:"mechanisms" default:"PLAIN"  desc:"config:kafka:mechanisms"`
	UseMock          bool   `yaml:"useMock" default:"false"  desc:"config:useMock"`
	Debug            string `yaml:"debug" default:"consumer"  desc:"config:kafka:debug"`
}
