package resp

import (
	"fmt"
	"io"
	"strconv"
)

func (d *Decoder) decodeBulkString() (Value, error) {

	// Read length
	line, err := d.readLine()
	if err != nil {
		return Value{}, err
	}

	length, err := strconv.Atoi(line)
	if err != nil {
		return Value{}, fmt.Errorf("invalid bulk string length %q: %w", line, err)
	}

	// RESP Null Bulk String
	if length == -1 {
		return Value{
			Type: BulkString,
		}, nil
	}

	if length < -1 {
		return Value{}, fmt.Errorf("invalid bulk string length %d", length)
	}

	// Read payload
	buf := make([]byte, length)

	_, err = io.ReadFull(d.reader, buf)
	if err != nil {
		return Value{}, err
	}

	// Consume trailing CRLF
	if err := d.expectCRLF(); err != nil {
		return Value{}, err
	}

	return NewBulkString(string(buf)), nil
}