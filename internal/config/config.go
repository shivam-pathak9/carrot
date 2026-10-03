// Package config defines and validates the runtime settings shared by both servers.
package config

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

// Config contains listener, resource-limit, and client-timeout settings.
type Config struct {
	Host             string
	Port             string
	MaxConnections   int
	MaxRequestBytes  int
	MaxResponseBytes int
	ReadTimeout      time.Duration
	WriteTimeout     time.Duration
	AOFEnabled       bool
	AOFPath          string
	AOFSyncPolicy    string
}

// DefaultConfig returns the server's baseline settings, including its bind address.
func DefaultConfig() Config {
	return Config{
		Host:             "0.0.0.0",
		Port:             "6379",
		MaxConnections:   128,
		MaxRequestBytes:  2 << 20,
		MaxResponseBytes: 4 << 20,
		ReadTimeout:      30 * time.Second,
		WriteTimeout:     10 * time.Second,
		AOFEnabled:       true,
		AOFPath:          "appendonly.aof",
		AOFSyncPolicy:    "everysec",
	}
}

// WithDefaults fills omitted operational settings while preserving explicitly
// provided network configuration.
func (c Config) WithDefaults() Config {
	defaults := DefaultConfig()
	if c.MaxConnections == 0 {
		c.MaxConnections = defaults.MaxConnections
	}
	if c.MaxRequestBytes == 0 {
		c.MaxRequestBytes = defaults.MaxRequestBytes
	}
	if c.MaxResponseBytes == 0 {
		c.MaxResponseBytes = defaults.MaxResponseBytes
	}
	if c.ReadTimeout == 0 {
		c.ReadTimeout = defaults.ReadTimeout
	}
	if c.WriteTimeout == 0 {
		c.WriteTimeout = defaults.WriteTimeout
	}
	if c.AOFPath == "" && c.AOFEnabled {
		c.AOFPath = defaults.AOFPath
	}
	if c.AOFSyncPolicy == "" {
		c.AOFSyncPolicy = defaults.AOFSyncPolicy
	}
	return c
}

// Validate rejects invalid addresses, ports, limits, and timeout values.
func (c Config) Validate() error {
	if c.Host == "" || strings.TrimSpace(c.Host) != c.Host {
		return fmt.Errorf("host must be a non-empty IP address or hostname")
	}
	if net.ParseIP(c.Host) == nil && !validHostname(c.Host) {
		return fmt.Errorf("invalid host %q", c.Host)
	}
	port, err := strconv.Atoi(c.Port)
	if err != nil || port < 0 || port > 65535 {
		return fmt.Errorf("port must be an integer between 0 and 65535")
	}
	if c.MaxConnections <= 0 {
		return fmt.Errorf("max connections must be greater than zero")
	}
	if c.MaxRequestBytes <= 0 {
		return fmt.Errorf("max request bytes must be greater than zero")
	}
	if c.MaxResponseBytes <= 0 {
		return fmt.Errorf("max response bytes must be greater than zero")
	}
	if c.ReadTimeout <= 0 {
		return fmt.Errorf("read timeout must be greater than zero")
	}
	if c.WriteTimeout <= 0 {
		return fmt.Errorf("write timeout must be greater than zero")
	}
	if c.AOFEnabled {
		if strings.TrimSpace(c.AOFPath) == "" {
			return fmt.Errorf("AOF path must not be empty when AOF is enabled")
		}
		switch c.AOFSyncPolicy {
		case "always", "everysec", "no":
		default:
			return fmt.Errorf("AOF sync policy must be one of: always, everysec, no")
		}
	}
	return nil
}

// IsWildcardHost reports whether host is an unspecified IP address such as 0.0.0.0.
func IsWildcardHost(host string) bool {
	ip := net.ParseIP(host)
	return ip != nil && ip.IsUnspecified()
}

func validHostname(host string) bool {
	if len(host) > 253 {
		return false
	}
	for _, label := range strings.Split(host, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, r := range label {
			if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '-' {
				return false
			}
		}
	}
	return true
}
