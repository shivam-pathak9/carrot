package resp

import (
	"bufio"
	"bytes"
	"strings"
	"testing"
)

// TestNewSimpleString tests creating a simple string value
func TestNewSimpleString(t *testing.T) {
	value := NewSimpleString("OK")
	if value.Type != SimpleString {
		t.Errorf("Expected type SimpleString, got %v", value.Type)
	}
	if value.String != "OK" {
		t.Errorf("Expected 'OK', got '%s'", value.String)
	}
}

// TestNewError tests creating an error value
func TestNewError(t *testing.T) {
	value := NewError("ERR unknown command")
	if value.Type != Error {
		t.Errorf("Expected type Error, got %v", value.Type)
	}
	if value.String != "ERR unknown command" {
		t.Errorf("Expected 'ERR unknown command', got '%s'", value.String)
	}
}

// TestNewInteger tests creating an integer value
func TestNewInteger(t *testing.T) {
	value := NewInteger(42)
	if value.Type != Integer {
		t.Errorf("Expected type Integer, got %v", value.Type)
	}
	if value.Integer != 42 {
		t.Errorf("Expected 42, got %d", value.Integer)
	}
}

// TestNewBulkString tests creating a bulk string value
func TestNewBulkString(t *testing.T) {
	value := NewBulkString("hello")
	if value.Type != BulkString {
		t.Errorf("Expected type BulkString, got %v", value.Type)
	}
	if value.String != "hello" {
		t.Errorf("Expected 'hello', got '%s'", value.String)
	}
	if value.IsNull {
		t.Error("Expected IsNull to be false")
	}
}

// TestNewNullBulkString tests creating a null bulk string
func TestNewNullBulkString(t *testing.T) {
	value := NewNullBulkString()
	if value.Type != BulkString {
		t.Errorf("Expected type BulkString, got %v", value.Type)
	}
	if !value.IsNull {
		t.Error("Expected IsNull to be true")
	}
}

// TestNewArray tests creating an array
func TestNewArray(t *testing.T) {
	value := NewArray(
		NewBulkString("PING"),
		NewBulkString("hello"),
	)
	if value.Type != Array {
		t.Errorf("Expected type Array, got %v", value.Type)
	}
	if len(value.Array) != 2 {
		t.Errorf("Expected array length 2, got %d", len(value.Array))
	}
}

// TestDecodeSimpleString tests decoding a simple string
func TestDecodeSimpleString(t *testing.T) {
	reader := bufio.NewReader(strings.NewReader("+OK\r\n"))
	decoder := NewDecoder(reader)

	value, err := decoder.Decode()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if value.Type != SimpleString {
		t.Errorf("Expected SimpleString, got %v", value.Type)
	}
	if value.String != "OK" {
		t.Errorf("Expected 'OK', got '%s'", value.String)
	}
}

// TestDecodeError tests decoding an error
func TestDecodeError(t *testing.T) {
	reader := bufio.NewReader(strings.NewReader("-ERR unknown command\r\n"))
	decoder := NewDecoder(reader)

	value, err := decoder.Decode()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if value.Type != Error {
		t.Errorf("Expected Error, got %v", value.Type)
	}
	if value.String != "ERR unknown command" {
		t.Errorf("Expected 'ERR unknown command', got '%s'", value.String)
	}
}

// TestDecodeInteger tests decoding an integer
func TestDecodeInteger(t *testing.T) {
	reader := bufio.NewReader(strings.NewReader(":1000\r\n"))
	decoder := NewDecoder(reader)

	value, err := decoder.Decode()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if value.Type != Integer {
		t.Errorf("Expected Integer, got %v", value.Type)
	}
	if value.Integer != 1000 {
		t.Errorf("Expected 1000, got %d", value.Integer)
	}
}

// TestDecodeNegativeInteger tests decoding a negative integer
func TestDecodeNegativeInteger(t *testing.T) {
	reader := bufio.NewReader(strings.NewReader(":-42\r\n"))
	decoder := NewDecoder(reader)

	value, err := decoder.Decode()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if value.Type != Integer {
		t.Errorf("Expected Integer, got %v", value.Type)
	}
	if value.Integer != -42 {
		t.Errorf("Expected -42, got %d", value.Integer)
	}
}

// TestDecodeBulkString tests decoding a bulk string
func TestDecodeBulkString(t *testing.T) {
	reader := bufio.NewReader(strings.NewReader("$5\r\nhello\r\n"))
	decoder := NewDecoder(reader)

	value, err := decoder.Decode()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if value.Type != BulkString {
		t.Errorf("Expected BulkString, got %v", value.Type)
	}
	if value.String != "hello" {
		t.Errorf("Expected 'hello', got '%s'", value.String)
	}
}

// TestDecodeNullBulkString tests decoding a null bulk string
func TestDecodeNullBulkString(t *testing.T) {
	reader := bufio.NewReader(strings.NewReader("$-1\r\n"))
	decoder := NewDecoder(reader)

	value, err := decoder.Decode()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if value.Type != BulkString {
		t.Errorf("Expected BulkString, got %v", value.Type)
	}
	if !value.IsNull {
		t.Error("Expected IsNull to be true")
	}
}

// TestDecodeArray tests decoding an array
func TestDecodeArray(t *testing.T) {
	reader := bufio.NewReader(strings.NewReader("*2\r\n$4\r\nPING\r\n$5\r\nhello\r\n"))
	decoder := NewDecoder(reader)

	value, err := decoder.Decode()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if value.Type != Array {
		t.Errorf("Expected Array, got %v", value.Type)
	}
	if len(value.Array) != 2 {
		t.Errorf("Expected array length 2, got %d", len(value.Array))
	}
	if value.Array[0].String != "PING" {
		t.Errorf("Expected first element 'PING', got '%s'", value.Array[0].String)
	}
	if value.Array[1].String != "hello" {
		t.Errorf("Expected second element 'hello', got '%s'", value.Array[1].String)
	}
}

// TestDecodeEmptyArray tests decoding an empty array
func TestDecodeEmptyArray(t *testing.T) {
	reader := bufio.NewReader(strings.NewReader("*0\r\n"))
	decoder := NewDecoder(reader)

	value, err := decoder.Decode()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if value.Type != Array {
		t.Errorf("Expected Array, got %v", value.Type)
	}
	if len(value.Array) != 0 {
		t.Errorf("Expected empty array, got length %d", len(value.Array))
	}
}

// TestEncodeSimpleString tests encoding a simple string
func TestEncodeSimpleString(t *testing.T) {
	buffer := &bytes.Buffer{}
	writer := bufio.NewWriter(buffer)
	encoder := NewEncoder(writer)

	value := NewSimpleString("OK")
	err := encoder.Encode(value)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	writer.Flush()

	expected := "+OK\r\n"
	if buffer.String() != expected {
		t.Errorf("Expected '%s', got '%s'", expected, buffer.String())
	}
}

// TestEncodeError tests encoding an error
func TestEncodeError(t *testing.T) {
	buffer := &bytes.Buffer{}
	writer := bufio.NewWriter(buffer)
	encoder := NewEncoder(writer)

	value := NewError("ERR unknown command")
	err := encoder.Encode(value)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	writer.Flush()

	expected := "-ERR unknown command\r\n"
	if buffer.String() != expected {
		t.Errorf("Expected '%s', got '%s'", expected, buffer.String())
	}
}

// TestEncodeInteger tests encoding an integer
func TestEncodeInteger(t *testing.T) {
	buffer := &bytes.Buffer{}
	writer := bufio.NewWriter(buffer)
	encoder := NewEncoder(writer)

	value := NewInteger(42)
	err := encoder.Encode(value)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	writer.Flush()

	expected := ":42\r\n"
	if buffer.String() != expected {
		t.Errorf("Expected '%s', got '%s'", expected, buffer.String())
	}
}

// TestEncodeBulkString tests encoding a bulk string
func TestEncodeBulkString(t *testing.T) {
	buffer := &bytes.Buffer{}
	writer := bufio.NewWriter(buffer)
	encoder := NewEncoder(writer)

	value := NewBulkString("hello")
	err := encoder.Encode(value)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	writer.Flush()

	expected := "$5\r\nhello\r\n"
	if buffer.String() != expected {
		t.Errorf("Expected '%s', got '%s'", expected, buffer.String())
	}
}

// TestEncodeNullBulkString tests encoding a null bulk string
func TestEncodeNullBulkString(t *testing.T) {
	buffer := &bytes.Buffer{}
	writer := bufio.NewWriter(buffer)
	encoder := NewEncoder(writer)

	value := NewNullBulkString()
	err := encoder.Encode(value)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	writer.Flush()

	expected := "$-1\r\n"
	if buffer.String() != expected {
		t.Errorf("Expected '%s', got '%s'", expected, buffer.String())
	}
}

// TestEncodeArray tests encoding an array
func TestEncodeArray(t *testing.T) {
	buffer := &bytes.Buffer{}
	writer := bufio.NewWriter(buffer)
	encoder := NewEncoder(writer)

	value := NewArray(
		NewBulkString("PING"),
		NewBulkString("hello"),
	)
	err := encoder.Encode(value)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	writer.Flush()

	expected := "*2\r\n$4\r\nPING\r\n$5\r\nhello\r\n"
	if buffer.String() != expected {
		t.Errorf("Expected '%s', got '%s'", expected, buffer.String())
	}
}

// TestRoundTripSimpleString tests encoding and then decoding a simple string
func TestRoundTripSimpleString(t *testing.T) {
	buffer := &bytes.Buffer{}
	writer := bufio.NewWriter(buffer)
	encoder := NewEncoder(writer)

	original := NewSimpleString("Hello")
	encoder.Encode(original)
	writer.Flush()

	reader := bufio.NewReader(buffer)
	decoder := NewDecoder(reader)
	decoded, err := decoder.Decode()

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if decoded.Type != original.Type {
		t.Errorf("Type mismatch: expected %v, got %v", original.Type, decoded.Type)
	}
	if decoded.String != original.String {
		t.Errorf("String mismatch: expected '%s', got '%s'", original.String, decoded.String)
	}
}

// TestRoundTripArray tests encoding and then decoding an array
func TestRoundTripArray(t *testing.T) {
	buffer := &bytes.Buffer{}
	writer := bufio.NewWriter(buffer)
	encoder := NewEncoder(writer)

	original := NewArray(
		NewBulkString("SET"),
		NewBulkString("key"),
		NewBulkString("value"),
	)
	encoder.Encode(original)
	writer.Flush()

	reader := bufio.NewReader(buffer)
	decoder := NewDecoder(reader)
	decoded, err := decoder.Decode()

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if decoded.Type != original.Type {
		t.Errorf("Type mismatch: expected %v, got %v", original.Type, decoded.Type)
	}
	if len(decoded.Array) != len(original.Array) {
		t.Errorf("Array length mismatch: expected %d, got %d", len(original.Array), len(decoded.Array))
	}
}

// TestEncodeBulkStringWithSpecialChars tests encoding bulk string with special characters
func TestEncodeBulkStringWithSpecialChars(t *testing.T) {
	testCases := []struct {
		input string
		name  string
	}{
		{"hello\nworld", "newline"},
		{"hello\r\nworld", "crlf"},
		{"hello\tworld", "tab"},
		{"hello world", "space"},
	}

	for _, tc := range testCases {
		buffer := &bytes.Buffer{}
		writer := bufio.NewWriter(buffer)
		encoder := NewEncoder(writer)

		value := NewBulkString(tc.input)
		err := encoder.Encode(value)
		if err != nil {
			t.Errorf("Encoding %s: unexpected error: %v", tc.name, err)
		}
		writer.Flush()

		// Decode and verify
		reader := bufio.NewReader(buffer)
		decoder := NewDecoder(reader)
		decoded, err := decoder.Decode()
		if err != nil {
			t.Errorf("Decoding %s: unexpected error: %v", tc.name, err)
		}
		if decoded.String != tc.input {
			t.Errorf("Round-trip %s: expected '%s', got '%s'", tc.name, tc.input, decoded.String)
		}
	}
}

// TestEncodeEmptyBulkString tests encoding an empty bulk string
func TestEncodeEmptyBulkString(t *testing.T) {
	buffer := &bytes.Buffer{}
	writer := bufio.NewWriter(buffer)
	encoder := NewEncoder(writer)

	value := NewBulkString("")
	err := encoder.Encode(value)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	writer.Flush()

	expected := "$0\r\n\r\n"
	if buffer.String() != expected {
		t.Errorf("Expected '%s', got '%s'", expected, buffer.String())
	}
}

// TestDecodeEmptyBulkString tests decoding an empty bulk string
func TestDecodeEmptyBulkString(t *testing.T) {
	reader := bufio.NewReader(strings.NewReader("$0\r\n\r\n"))
	decoder := NewDecoder(reader)

	value, err := decoder.Decode()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if value.Type != BulkString {
		t.Errorf("Expected BulkString, got %v", value.Type)
	}
	if value.String != "" {
		t.Errorf("Expected empty string, got '%s'", value.String)
	}
}

// TestDecodeNestedArray tests decoding nested arrays
func TestDecodeNestedArray(t *testing.T) {
	// Array with nested array
	reader := bufio.NewReader(strings.NewReader("*2\r\n*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$5\r\nhello\r\n"))
	decoder := NewDecoder(reader)

	value, err := decoder.Decode()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if value.Type != Array {
		t.Errorf("Expected Array, got %v", value.Type)
	}
	if len(value.Array) != 2 {
		t.Errorf("Expected array length 2, got %d", len(value.Array))
	}
	if value.Array[0].Type != Array {
		t.Errorf("Expected first element to be Array, got %v", value.Array[0].Type)
	}
}

// TestEncodeLargeInteger tests encoding large integers
func TestEncodeLargeInteger(t *testing.T) {
	buffer := &bytes.Buffer{}
	writer := bufio.NewWriter(buffer)
	encoder := NewEncoder(writer)

	value := NewInteger(9223372036854775807) // Max int64
	err := encoder.Encode(value)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	writer.Flush()

	// Decode and verify
	reader := bufio.NewReader(buffer)
	decoder := NewDecoder(reader)
	decoded, err := decoder.Decode()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if decoded.Integer != 9223372036854775807 {
		t.Errorf("Expected 9223372036854775807, got %d", decoded.Integer)
	}
}

// TestEncodeMultipleElements tests encoding multiple types in sequence
func TestEncodeMultipleElements(t *testing.T) {
	buffer := &bytes.Buffer{}
	writer := bufio.NewWriter(buffer)
	encoder := NewEncoder(writer)

	values := []Value{
		NewSimpleString("OK"),
		NewError("ERR"),
		NewInteger(42),
		NewBulkString("test"),
	}

	for _, v := range values {
		err := encoder.Encode(v)
		if err != nil {
			t.Fatalf("Unexpected error encoding: %v", err)
		}
	}
	writer.Flush()

	// Decode all elements
	reader := bufio.NewReader(buffer)
	decoder := NewDecoder(reader)

	for i, expected := range values {
		decoded, err := decoder.Decode()
		if err != nil {
			t.Fatalf("Unexpected error decoding element %d: %v", i, err)
		}
		if decoded.Type != expected.Type {
			t.Errorf("Element %d: type mismatch", i)
		}
	}
}
