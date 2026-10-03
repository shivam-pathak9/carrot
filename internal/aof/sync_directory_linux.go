//go:build linux

package aof

import (
	"errors"
	"os"
	"path/filepath"
)

// syncDirectory persists the directory entry created for a new AOF. Syncing
// the file alone does not guarantee that the newly created filename survives
// a system crash.
func syncDirectory(path string) error {
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	return errors.Join(syncErr, closeErr)
}
