package config

// WithDefaults applies sensible defaults for segmentio/kafka-go client configuration.
func (c *KafkaConfig) WithDefaults() *KafkaConfig {
	if c == nil {
		return nil
	}
	if c.Brokers == "" {
		c.Brokers = "127.0.0.1:9092"
	}
	if c.Username != "" && c.SecurityProtocol == "" {
		c.SecurityProtocol = "SASL_SSL"
	}
	if c.Username == "" && c.SecurityProtocol == "" {
		c.SecurityProtocol = "PLAINTEXT"
	}
	return c
}
