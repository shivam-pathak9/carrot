package config

import (
	"strings"
	"testing"
)

// TestDefaultConfig tests the default configuration
func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.Host == "" {
		t.Error("Expected non-empty Host")
	}
	if cfg.Port == "" {
		t.Error("Expected non-empty Port")
	}
}

// TestDefaultConfigHost tests default host value
func TestDefaultConfigHost(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.Host != "0.0.0.0" {
		t.Errorf("Expected host '0.0.0.0', got '%s'", cfg.Host)
	}
}

// TestDefaultConfigPort tests default port value
func TestDefaultConfigPort(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.Port != "6379" {
		t.Errorf("Expected port '6379', got '%s'", cfg.Port)
	}
}

func TestDefaultOperationalConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.MaxConnections != 128 {
		t.Errorf("MaxConnections = %d, want 128", cfg.MaxConnections)
	}
	if cfg.MaxRequestBytes != 2<<20 {
		t.Errorf("MaxRequestBytes = %d, want %d", cfg.MaxRequestBytes, 2<<20)
	}
	if cfg.MaxResponseBytes != 4<<20 {
		t.Errorf("MaxResponseBytes = %d, want %d", cfg.MaxResponseBytes, 4<<20)
	}
	if cfg.ReadTimeout <= 0 || cfg.WriteTimeout <= 0 {
		t.Errorf("timeouts must be positive: read=%s write=%s", cfg.ReadTimeout, cfg.WriteTimeout)
	}
	if !cfg.AOFEnabled || cfg.AOFPath != "appendonly.aof" || cfg.AOFSyncPolicy != "everysec" {
		t.Errorf("unexpected AOF defaults: enabled=%t path=%q sync=%q", cfg.AOFEnabled, cfg.AOFPath, cfg.AOFSyncPolicy)
	}
}

func TestConfigValidation(t *testing.T) {
	valid := DefaultConfig()
	if err := valid.Validate(); err != nil {
		t.Fatalf("Validate(default) error = %v", err)
	}
	for _, test := range []struct {
		name string
		edit func(*Config)
		want string
	}{
		{"empty host", func(c *Config) { c.Host = "" }, "host"},
		{"invalid host", func(c *Config) { c.Host = "bad host" }, "host"},
		{"invalid port", func(c *Config) { c.Port = "abc" }, "port"},
		{"port out of range", func(c *Config) { c.Port = "65536" }, "port"},
		{"zero connections", func(c *Config) { c.MaxConnections = 0 }, "max connections"},
		{"negative request limit", func(c *Config) { c.MaxRequestBytes = -1 }, "max request"},
		{"negative response limit", func(c *Config) { c.MaxResponseBytes = -1 }, "max response"},
		{"zero read timeout", func(c *Config) { c.ReadTimeout = 0 }, "read timeout"},
		{"negative write timeout", func(c *Config) { c.WriteTimeout = -1 }, "write timeout"},
		{"empty AOF path", func(c *Config) { c.AOFPath = " " }, "AOF path"},
		{"invalid AOF sync policy", func(c *Config) { c.AOFSyncPolicy = "sometimes" }, "AOF sync policy"},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := valid
			test.edit(&cfg)
			err := cfg.Validate()
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Validate() error = %v, want message containing %q", err, test.want)
			}
		})
	}
}

func TestConfigWithDefaults(t *testing.T) {
	cfg := (Config{Host: "127.0.0.1", Port: "16379"}).WithDefaults()
	if cfg.Host != "127.0.0.1" || cfg.Port != "16379" {
		t.Fatalf("WithDefaults changed address: %+v", cfg)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate(WithDefaults) error = %v", err)
	}
}

func TestIsWildcardHost(t *testing.T) {
	for host, want := range map[string]bool{
		"0.0.0.0":   true,
		"::":        true,
		"::0":       true,
		"127.0.0.1": false,
		"localhost": false,
	} {
		if got := IsWildcardHost(host); got != want {
			t.Errorf("IsWildcardHost(%q) = %t, want %t", host, got, want)
		}
	}
}

// TestConfigCreation tests creating a custom configuration
func TestConfigCreation(t *testing.T) {
	cfg := Config{
		Host: "localhost",
		Port: "9999",
	}

	if cfg.Host != "localhost" {
		t.Errorf("Expected host 'localhost', got '%s'", cfg.Host)
	}
	if cfg.Port != "9999" {
		t.Errorf("Expected port '9999', got '%s'", cfg.Port)
	}
}

// TestConfigCustomHost tests custom host configuration
func TestConfigCustomHost(t *testing.T) {
	testCases := []string{
		"127.0.0.1",
		"192.168.1.1",
		"localhost",
		"redis.example.com",
		"::",
	}

	for _, host := range testCases {
		cfg := Config{
			Host: host,
			Port: "6379",
		}
		if cfg.Host != host {
			t.Errorf("Expected host '%s', got '%s'", host, cfg.Host)
		}
	}
}

// TestConfigCustomPort tests custom port configuration
func TestConfigCustomPort(t *testing.T) {
	testCases := []string{
		"3000",
		"8080",
		"9999",
		"6380",
		"12345",
	}

	for _, port := range testCases {
		cfg := Config{
			Host: "localhost",
			Port: port,
		}
		if cfg.Port != port {
			t.Errorf("Expected port '%s', got '%s'", port, cfg.Port)
		}
	}
}

// TestConfigEmptyHost tests config with empty host
func TestConfigEmptyHost(t *testing.T) {
	cfg := Config{
		Host: "",
		Port: "6379",
	}

	if cfg.Host != "" {
		t.Errorf("Expected empty host, got '%s'", cfg.Host)
	}
}

// TestConfigEmptyPort tests config with empty port
func TestConfigEmptyPort(t *testing.T) {
	cfg := Config{
		Host: "localhost",
		Port: "",
	}

	if cfg.Port != "" {
		t.Errorf("Expected empty port, got '%s'", cfg.Port)
	}
}

// TestConfigAllLocalhost tests localhost configuration
func TestConfigAllLocalhost(t *testing.T) {
	cfg := Config{
		Host: "127.0.0.1",
		Port: "6379",
	}

	if cfg.Host != "127.0.0.1" {
		t.Errorf("Expected '127.0.0.1', got '%s'", cfg.Host)
	}
	if cfg.Port != "6379" {
		t.Errorf("Expected '6379', got '%s'", cfg.Port)
	}
}

// TestConfigAllInterfaces tests listening on all interfaces
func TestConfigAllInterfaces(t *testing.T) {
	cfg := Config{
		Host: "0.0.0.0",
		Port: "6379",
	}

	if cfg.Host != "0.0.0.0" {
		t.Errorf("Expected '0.0.0.0', got '%s'", cfg.Host)
	}
	if cfg.Port != "6379" {
		t.Errorf("Expected '6379', got '%s'", cfg.Port)
	}
}
