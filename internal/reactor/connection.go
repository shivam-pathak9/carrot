//go:build linux

package reactor

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"

	"github.com/shivampathak/carrot/internal/command"
	"github.com/shivampathak/carrot/internal/protocol/resp"
	"golang.org/x/sys/unix"
)

// Connection represents a non-blocking client TCP socket managed by the Reactor.
type Connection struct {
	fd       int
	poller   *Poller
	parser   *command.Parser
	executor *command.Executor

	inBuf  bytes.Buffer
	outBuf bytes.Buffer

	isClosed bool
}

// NewConnection creates a new Connection instance for the given socket file descriptor.
func NewConnection(fd int, poller *Poller, parser *command.Parser, executor *command.Executor) *Connection {
	return &Connection{
		fd:       fd,
		poller:   poller,
		parser:   parser,
		executor: executor,
	}
}

// FD returns the file descriptor of the connection.
func (c *Connection) FD() int {
	return c.fd
}

// OnRead reads incoming data from the non-blocking socket and processes complete RESP commands.
func (c *Connection) OnRead() error {
	if c.isClosed {
		return errors.New("connection closed")
	}

	buf := make([]byte, 4096)
	for {
		n, err := unix.Read(c.fd, buf)
		if n > 0 {
			c.inBuf.Write(buf[:n])
		}

		if err != nil {
			if errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EWOULDBLOCK) {
				// Socket read buffer exhausted for now.
				break
			}
			if errors.Is(err, unix.EINTR) {
				continue
			}
			return fmt.Errorf("read error on fd %d: %w", c.fd, err)
		}

		if n == 0 {
			// Remote client closed connection (EOF).
			return io.EOF
		}
	}

	// Process any complete commands available in c.inBuf
	return c.processCommands()
}

// processCommands attempts to parse and execute RESP commands from c.inBuf.
func (c *Connection) processCommands() error {
	for c.inBuf.Len() > 0 {
		raw := c.inBuf.Bytes()
		reader := bytes.NewReader(raw)
		bufReader := bufio.NewReader(reader)
		decoder := resp.NewDecoder(bufReader)

		value, err := decoder.Decode()
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				// Partial command in buffer; wait for more data.
				break
			}
			// Protocol error or corrupted payload
			respErr := resp.NewError(fmt.Sprintf("ERR protocol error: %v", err))
			c.writeResponse(respErr)
			c.inBuf.Reset()
			return c.Flush()
		}

		// Calculate how many bytes were consumed for this value
		consumed := len(raw) - reader.Len()
		c.inBuf.Next(consumed)

		// Parse RESP value into Command
		cmd, err := c.parser.Parse(value)
		if err != nil {
			respErr := resp.NewError(err.Error())
			c.writeResponse(respErr)
			continue
		}

		// Execute command
		response, err := c.executor.Execute(cmd)
		if err != nil {
			respErr := resp.NewError(err.Error())
			c.writeResponse(respErr)
			continue
		}

		// Write response
		c.writeResponse(response)
	}

	return c.Flush()
}

// writeResponse encodes a RESP Value into the outbound buffer.
func (c *Connection) writeResponse(val resp.Value) {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	encoder := resp.NewEncoder(writer)
	if err := encoder.Encode(val); err == nil {
		_ = writer.Flush()
		c.outBuf.Write(buf.Bytes())
	}
}

// Flush attempts to send outbound buffer bytes to the non-blocking socket.
func (c *Connection) Flush() error {
	for c.outBuf.Len() > 0 {
		n, err := unix.Write(c.fd, c.outBuf.Bytes())
		if n > 0 {
			c.outBuf.Next(n)
		}

		if err != nil {
			if errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EWOULDBLOCK) {
				// Socket write buffer full; enable EPOLLOUT interest
				return c.poller.Modify(c.fd, DefaultEventMask|unix.EPOLLOUT)
			}
			if errors.Is(err, unix.EINTR) {
				continue
			}
			return fmt.Errorf("write error on fd %d: %w", c.fd, err)
		}
	}

	// All outbound bytes sent; reset interest to default (without EPOLLOUT)
	return c.poller.Modify(c.fd, DefaultEventMask)
}

// OnWrite handles socket write readiness (EPOLLOUT).
func (c *Connection) OnWrite() error {
	if c.isClosed {
		return errors.New("connection closed")
	}

	return c.Flush()
}

// RemoteAddr returns the remote network address of the connection.
func (c *Connection) RemoteAddr() net.Addr {
	sa, err := unix.Getpeername(c.fd)
	if err != nil {
		return nil
	}

	switch sa := sa.(type) {
	case *unix.SockaddrInet4:
		return &net.TCPAddr{
			IP:   sa.Addr[:],
			Port: sa.Port,
		}
	case *unix.SockaddrInet6:
		return &net.TCPAddr{
			IP:   sa.Addr[:],
			Port: sa.Port,
		}
	}
	return nil
}

// Close closes the connection socket and unregisters it from the poller.
func (c *Connection) Close() error {
	if c.isClosed {
		return nil
	}
	c.isClosed = true

	_ = c.poller.Unregister(c.fd)
	err := unix.Close(c.fd)
	if err != nil && !errors.Is(err, syscall.EBADF) {
		return fmt.Errorf("failed to close fd %d: %w", c.fd, err)
	}
	return nil
}
