package config

// WithDefaults applies sensible defaults for amqp091-go client configuration.
// All time-based values are in **seconds** to match time.Duration usage.
func (c *RabbitMQConfig) WithDefaults() *RabbitMQConfig {
	if c == nil {
		return nil
	}
	if c.Host == "" {
		c.Host = "127.0.0.1"
	}
	if c.Port == 0 {
		c.Port = 5672
	}
	if c.Username == "" {
		c.Username = "guest"
	}
	if c.Password == "" {
		c.Password = "guest"
	}
	if c.ReconnectDuration == 0 {
		c.ReconnectDuration = 5
	}
	if c.ConnectTimeout == 0 {
		c.ConnectTimeout = 10
	}
	if c.PublishTimeout == 0 {
		c.PublishTimeout = 5
	}
	return c
}
