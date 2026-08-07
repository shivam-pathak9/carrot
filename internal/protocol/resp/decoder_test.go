package resp

import (
	"bufio"
	"strings"
	"testing"
)

func TestDecodeInlineCommand(t *testing.T) {
	reader := bufio.NewReader(strings.NewReader("PING\r\n"))
	decoder := NewDecoder(reader)

	value, err := decoder.Decode()
	if err != nil {
		t.Fatalf("Decode() returned error: %v", err)
	}

	if value.Type != Array {
		t.Fatalf("expected Array value, got %v", value.Type)
	}

	if len(value.Array) != 1 {
		t.Fatalf("expected 1 element, got %d", len(value.Array))
	}

	if value.Array[0].Type != BulkString {
		t.Fatalf("expected first element to be BulkString, got %v", value.Array[0].Type)
	}

	if value.Array[0].String != "PING" {
		t.Fatalf("expected command name PING, got %q", value.Array[0].String)
	}
}
