//go:build linux

package aof

import (
	"os"
	"syscall"
)

// ensurePlatform confirms this build has the Linux locking and directory-sync
// operations required by the AOF durability contract.
func ensurePlatform() error { return nil }

// lockFile takes a non-blocking exclusive advisory lock so two Carrot
// processes cannot recover or append to the same AOF at the same time.
func lockFile(file *os.File) error {
	return syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
}

// unlockFile releases the advisory lock acquired by lockFile.
func unlockFile(file *os.File) error {
	return syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
}
