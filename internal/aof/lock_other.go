//go:build !linux

package aof

import (
	"fmt"
	"os"
)

// ensurePlatform rejects AOF before it creates or changes a file on a platform
// where the required durability operations are unavailable.
func ensurePlatform() error {
	return fmt.Errorf("AOF persistence is currently supported only on Linux")
}

// lockFile fails closed: AOF must not run without a platform-supported
// exclusive lock because concurrent processes could corrupt the log.
func lockFile(*os.File) error {
	return fmt.Errorf("AOF persistence is currently supported only on Linux")
}

// unlockFile is present to keep the platform-specific helper API consistent.
func unlockFile(*os.File) error {
	return fmt.Errorf("AOF persistence is currently supported only on Linux")
}
