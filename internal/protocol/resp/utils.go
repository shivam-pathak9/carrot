package resp

import (
	"fmt"
)

func (d *Decoder) readLine() (string, error) {
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