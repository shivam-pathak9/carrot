package resp

import (
	"bufio"
	"errors"
	"fmt"
)

const (
	maxRESPLineLength   = 16 * 1024
	maxBulkStringLength = 1 << 20
	maxArrayLength      = 1024
)

func (d *Decoder) readLine() (string, error) {
	// readLine reads a single CRLF-terminated line without the terminating CRLF.
	// It bounds the total line length so a malicious client cannot force us to
	// allocate unbounded memory while reading a response header.
	var line []byte
	for {
		chunk, err := d.reader.ReadSlice('\n')
		if len(chunk) > 0 {
			line = append(line, chunk...)
			if len(line) > maxRESPLineLength {
				return "", fmt.Errorf("RESP line exceeds %d bytes", maxRESPLineLength)
			}
		}

		if err == nil {
			break
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if err != nil {
			return "", err
		}
	}

	n := len(line)
	if n < 2 || line[n-2] != '\r' || line[n-1] != '\n' {
		return "", fmt.Errorf("invalid RESP line: expected CRLF")
	}

	return string(line[:n-2]), nil
}

func (d *Decoder) expectCRLF() error {
	// expectCRLF consumes the next two bytes and verifies they are
	// CR and LF. It is used after reading fixed-length payloads
	// (e.g. bulk string bodies) to consume the terminating CRLF.
	// Reading and validating these two bytes here keeps callers
	// simpler (they don't need to remember to strip the CRLF).
	// CLRF == /r/n
	cr, err := d.reader.ReadByte()
	if err != nil {
		return err
	}

	lf, err := d.reader.ReadByte()
	if err != nil {
		return err
	}

	if cr != '\r' || lf != '\n' {
		return fmt.Errorf("invalid RESP termination")
	}

	return nil
}
