package config

type Config struct {
	Host string
	Port string
}

func DefaultConfig() Config {
	// DefaultConfig returns a sane set of defaults for running the
	// server locally (listen on all interfaces, default Redis port).
	return Config{
		Host: "0.0.0.0",
		Port: "6379",
	}
}
