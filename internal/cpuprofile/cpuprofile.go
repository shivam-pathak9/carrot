package cpuprofile

import (
	"errors"
	"os"
	"runtime/pprof"
)

// Start begins CPU profiling when path is non-empty. The returned function
// stops profiling and closes the profile file.
func Start(path string) (func() error, error) {
	if path == "" {
		return func() error { return nil }, nil
	}
	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	if err := pprof.StartCPUProfile(file); err != nil {
		return nil, errors.Join(err, file.Close())
	}
	return func() error {
		pprof.StopCPUProfile()
		return file.Close()
	}, nil
}
