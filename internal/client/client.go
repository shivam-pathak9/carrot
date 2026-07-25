package client

import (
	"bufio"
	"github.com/shivampathak/carrot/internal/protocol/resp"
	"net"
)

// conn : The underlying TCP connection.
// reader : Buffered reads. This is what our RESP parser will use.
// writer : Buffered writes. This will be used by our RESP encoder.
type Client struct {
	conn    net.Conn
	reader  *bufio.Reader
	writer  *bufio.Writer
	decoder *resp.Decoder
	encoder *resp.Encoder
}

func NewClient(conn net.Conn) *Client {
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	return &Client{
		// store the underlying connection so callers can access
		// connection-level info (remote addr) and close it later.
		// NOTE: omitting this field leads to nil-pointer panics
		// from methods like `Close()` or `RemoteAddr()`.
		conn: conn,

		reader: reader,
		writer: writer,

		decoder: resp.NewDecoder(reader),
		encoder: resp.NewEncoder(writer),
	}
}

// Client represents a single client connection to the server.
// It wraps the raw `net.Conn` with buffered reader/writer and
// RESP encoder/decoder helpers to read and write RESP protocol
// messages efficiently.

// conn : The underlying TCP connection.
// reader : Buffered reads. This is what our RESP parser will use.
// writer : Buffered writes. This will be used by our RESP encoder.

func (c *Client) Read(p []byte) (int, error) {
	return c.reader.Read(p)
}

func (c *Client) Write(p []byte) (int, error) {
	n, err := c.writer.Write(p)
	if err != nil {
		return n, err
	}

	// We flush after write to ensure the bytes are actually sent
	// to the network. The encoder writes RESP messages into the
	// buffered writer; without flushing the data may remain in
	// memory and the remote client won't receive the response.
	//
	// Flushing on every small write has a throughput cost, which
	// is why higher-level code (server) typically writes a full
	// RESP message using the encoder and then calls `Flush()`
	// once. The `Write` helper flushes here to make it safe for
	// callers that use raw writes directly.
	if err := c.writer.Flush(); err != nil {
		return n, err
	}

	return n, nil
}

func (c *Client) Close() error {
	// Close the underlying network connection. This will also
	// cause any pending reads/writes to error out for this client.
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

func (c *Client) Flush() error {
	// Flush any buffered data to the underlying connection.
	// Use this after encoding one or more RESP values when you
	// want to ensure the response is transmitted right away.
	//
	// Rationale: buffering reduces syscalls and improves
	// throughput. However, protocol semantics require responses
	// be sent after handling a request, therefore the server
	// explicitly calls `Flush()` once per reply.
	return c.writer.Flush()
}

func (c *Client) RemoteAddr() net.Addr {
	// Return the remote address of the client connection. If the
	// underlying connection is nil, return nil to avoid panics.
	if c.conn == nil {
		return nil
	}
	return c.conn.RemoteAddr()
}

func (c *Client) Encoder() *resp.Encoder {
	// Accessor for the RESP encoder. Callers use this to write
	// RESP messages (arrays, bulk strings, errors, etc.) to the
	// buffered writer. Remember to `Flush()` after writing when
	// immediate delivery is required.
	return c.encoder
}
func (c *Client) Decoder() *resp.Decoder {
	// Accessor for the RESP decoder. Use this to read and decode
	// incoming RESP messages from the buffered reader.
	return c.decoder
}
