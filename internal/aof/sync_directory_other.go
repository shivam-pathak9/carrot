//go:build !linux

package aof

import "fmt"

// syncDirectory fails closed because this implementation has no portable way
// to guarantee that a newly created AOF directory entry is durable.
func syncDirectory(string) error {
	return fmt.Errorf("AOF persistence is currently supported only on Linux")
}
