package config

import (
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
