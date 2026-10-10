package cpuprofile

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestStartWritesCPUProfile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cpu.pprof")
	stop, err := Start(path)
	if err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(25 * time.Millisecond)
	var value uint64
	for time.Now().Before(deadline) {
		value = value*1664525 + 1013904223
	}
	if value == 0 {
		t.Fatal("profile workload did not execute")
	}
	if err := stop(); err != nil {
		t.Fatalf("stop CPU profile: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() == 0 {
		t.Fatal("CPU profile is empty")
	}
}

func TestStartWithoutPathReturnsNoopStop(t *testing.T) {
	stop, err := Start("")
	if err != nil {
		t.Fatal(err)
	}
	if err := stop(); err != nil {
		t.Fatal(err)
	}
}
