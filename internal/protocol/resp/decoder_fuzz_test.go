package resp

import (
	"bufio"
	"bytes"
	"testing"
)

func FuzzDecoderDoesNotPanic(f *testing.F) {
	for _, seed := range [][]byte{
		{},
		[]byte("+OK\r\n"),
		[]byte("*1\r\n$4\r\nPING\r\n"),
		[]byte("$-1\r\n"),
		[]byte("*999999999\r\n"),
		[]byte("*1\r\n*1\r\n*1\r\n"),
		[]byte("PING\r\n"),
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		const maxInputBytes = 64 << 10
		if len(data) > maxInputBytes {
			return
		}

		decoder := NewDecoderWithLimit(
			bufio.NewReader(bytes.NewReader(data)),
			maxInputBytes,
		)
		for decoded := 0; decoded < 256; decoded++ {
			if _, err := decoder.Decode(); err != nil {
				return
			}
		}
	})
}
