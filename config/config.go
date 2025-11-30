package config

// Listener default config
type Listener struct {
	Listen string `yaml:"listen" default:"0.0.0.0"`
	Port   int    `yaml:"port" default:"8081"`
}

type SQLConfig struct {
	Enable   bool   `yaml:"enable" json:"enable" default:"true"`
	Host     string `yaml:"host" json:"host"`
	Port     int    `yaml:"port" json:"port" default:"5432"`
	Username string `yaml:"username" json:"username"`
	Password string `yaml:"password" json:"password"`
	Database string `yaml:"database" json:"database"`
	Options  string `yaml:"options" json:"options"`

	// Full override DSN
	Connection string `yaml:"connection" json:"connection"`

	// Pool Settings (pgxpool)
	MaxConn           int `yaml:"max_conn" json:"max_conn" default:"0"`                        // 0 = auto (CPU*8)
	MinConn           int `yaml:"min_conn" json:"min_conn" default:"0"`                        // 0 = auto (CPU)
	MaxConnLifetime   int `yaml:"max_conn_lifetime" json:"max_conn_lifetime" default:"60"`     // menit
	MaxConnIdleTime   int `yaml:"max_conn_idle_time" json:"max_conn_idle_time" default:"10"`   // menit
	HealthCheckPeriod int `yaml:"health_check_period" json:"health_check_period" default:"15"` // detik

	ConnectTimeout int `yaml:"connect_timeout" json:"connect_timeout" default:"10"` // detik

	// Behavior
	AutoReconnect bool `yaml:"auto_reconnect" json:"auto_reconnect" default:"true"`

	// Untuk future / extensibility
	UseMock bool `yaml:"use_mock" json:"use_mock" default:"false"`
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
