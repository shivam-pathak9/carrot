package client

import (
	"bufio"
	"net"
)

// conn : The underlying TCP connection.
// reader : Buffered reads. This is what our RESP parser will use.
// writer : Buffered writes. This will be used by our RESP encoder.
type Client struct {
	conn   net.Conn
	reader *bufio.Reader
	writer *bufio.Writer
}

func NewClient(conn net.Conn) *Client {
	return &Client{
		conn:   conn,
		reader: bufio.NewReader(conn),
		writer: bufio.NewWriter(conn),
	}
}


func (c *Client) Read(p []byte) (int, error) {
	return c.reader.Read(p)
}

func (c *Client) Write(p []byte) (int, error) {
	n, err := c.writer.Write(p)
	if err != nil {
		return n, err
	}

	if err := c.writer.Flush(); err != nil {
		return n, err
	}

	return n, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}