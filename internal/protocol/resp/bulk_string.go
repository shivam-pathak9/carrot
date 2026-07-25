package resp

import (
	"fmt"
	"io"
	"strconv"
)

func (d *Decoder) decodeBulkString() (Value, error) {

	// decodeBulkString reads a bulk string: it first reads the
	// length line, then reads that many bytes of payload and
	// consumes the terminating CRLF. Supports NULL bulk string
	// (length == -1).
	// Read length
	// decodeBulkString reads a RESP Bulk String. RESP bulk strings
	// are encoded as: `$<length>\r\n<payload>\r\n`.
	//
	// Steps performed here:
	// 1. Read the length line (without CRLF) using `readLine()`.
	// 2. Parse the length. The special length `-1` indicates a
	//    NULL bulk string, which we represent as a Value with
	//    Type==BulkString and an empty payload.
	// 3. For non-negative lengths, allocate a buffer of exactly
	//    length bytes and use io.ReadFull to ensure we read the
	//    full payload; this avoids returning partial data when the
	//    connection is interrupted.
	// 4. After reading the payload bytes we must consume the
	//    trailing CRLF using `expectCRLF()`; failure to do so would
	//    leave the reader in an inconsistent state for the next
	//    decode call.
	//
	// Note: converting the read bytes into a Go string performs a
	// copy; for very large bulk strings this impacts memory.
	//
	// Using explicit length handling + `io.ReadFull` is required
	// because RESP payloads may contain embedded newlines and
	// cannot be read line-oriented.
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
