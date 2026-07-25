package resp

import (
	"fmt"
)

func (d *Decoder) readLine() (string, error) {
	// readLine reads a single CRLF-terminated line (without the CRLF)
	// and validates that the line ends with CRLF as required by RESP.
	// It uses `ReadString('\n')` which returns data up to and
	// including the trailing '\n'. We validate the preceding byte
	// is '\r' to ensure correct CRLF termination.
	line, err := d.reader.ReadString('\n')
	if err != nil {
		return "", err
	}

	n := len(line)

	if n < 2 || line[n-2] != '\r' || line[n-1] != '\n' {
		return "", fmt.Errorf("invalid RESP line: expected CRLF")
	}

	return line[:n-2], nil
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
