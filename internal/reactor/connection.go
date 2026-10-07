//go:build linux

// connection.go implements each non-blocking client socket's read/execute/write cycle.
package reactor

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"
	"time"

	"github.com/shivam-pathak9/carrot/internal/command"
	"github.com/shivam-pathak9/carrot/internal/protocol/resp"
	"golang.org/x/sys/unix"
)

// Connection represents a non-blocking client TCP socket managed by the single-threaded Reactor EventLoop.
//
// Struct Fields & Why they are part of Connection:
//   - fd       : The OS file descriptor integer for this specific client socket (e.g. fd=5).
//     Used for non-blocking read (`unix.Read`) and write (`unix.Write`) operations.
//   - poller   : Pointer to shared Poller instance. Needed to dynamically modify epoll interest flags
//     (e.g., adding EPOLLOUT when outbound buffer cannot be flushed immediately).
//   - parser   : Pointer to shared command.Parser. Parses decoded RESP Values into executable Command objects.
//   - executor : Pointer to shared command.Executor. Executes commands (like PING) and returns RESP response Values.
//   - inBuf    : Memory byte buffer accumulating unparsed incoming bytes received from non-blocking socket reads.
//     Crucial for handling partial TCP frame delivery (TCP segmentation).
//   - outBuf   : Memory byte buffer queuing serialized RESP response bytes waiting to be sent to client socket.
//     Crucial for non-blocking socket writes when OS TCP socket send buffer is temporarily full.
//   - isClosed : Guard flag ensuring idempotent close operations and preventing operations on closed FDs.
const (
	defaultMaxInboundBufferSize  = 2 << 20
	defaultMaxOutboundBufferSize = 4 << 20
	defaultIdleTimeout           = 30 * time.Second
	defaultWriteTimeout          = 10 * time.Second
)

type Connection struct {
	fd       int               // OS File descriptor representing client socket
	poller   *Poller           // Epoll handle to update event interest masks
	parser   *command.Parser   // Shared RESP -> Command parser
	executor *command.Executor // Shared Command -> RESP response executor

	inBuf        bytes.Buffer // Inbound stream buffer (accumulates partial TCP frames)
	outBuf       bytes.Buffer // Outbound stream buffer (queues pending socket writes)
	lastActivity time.Time    // Tracks last successful read/write activity for idle disconnects
	writeStarted time.Time
	maxInbound   int
	maxOutbound  int
	idleTimeout  time.Duration
	writeTimeout time.Duration

	isClosed bool // Closed status safety flag
}

// NewConnection constructs a new Connection object wrapping an accepted non-blocking client file descriptor.
//
// Parameters:
//   - fd      : The newly accepted socket file descriptor (returned by unix.Accept4).
//   - poller  : Reference to reactor epoll instance.
//   - parser  : Reference to command parser.
//   - executor: Reference to command executor.
func NewConnection(fd int, poller *Poller, parser *command.Parser, executor *command.Executor) *Connection {
	return NewConnectionWithLimits(fd, poller, parser, executor, defaultMaxInboundBufferSize, defaultMaxOutboundBufferSize, defaultIdleTimeout, defaultWriteTimeout)
}

// NewConnectionWithLimits wraps fd with explicit inbound/outbound and timeout bounds.
func NewConnectionWithLimits(fd int, poller *Poller, parser *command.Parser, executor *command.Executor, maxInbound, maxOutbound int, idleTimeout, writeTimeout time.Duration) *Connection {
	return &Connection{
		fd:           fd,
		poller:       poller,
		parser:       parser,
		executor:     executor,
		lastActivity: time.Now(),
		maxInbound:   maxInbound,
		maxOutbound:  maxOutbound,
		idleTimeout:  idleTimeout,
		writeTimeout: writeTimeout,
	}
}

func (c *Connection) expired(now time.Time) bool {
	if now.Sub(c.lastActivity) >= c.idleTimeout {
		return true
	}
	return c.outBuf.Len() > 0 && now.Sub(c.writeStarted) >= c.writeTimeout
}

// FD returns the underlying numeric OS socket file descriptor.
func (c *Connection) FD() int {
	return c.fd
}

// OnRead drains available socket data and converts it into RESP commands when a full request is present.
//
// The reactor reads into a fixed-size scratch buffer until the socket reports no more readable bytes,
// then it tries to parse and execute any complete RESP frames already buffered in memory. Partial frames
// are left in c.inBuf until more data arrives.
func (c *Connection) OnRead() error {
	if c.isClosed {
		return errors.New("connection closed")
	}
	if time.Since(c.lastActivity) >= c.idleTimeout {
		return fmt.Errorf("idle timeout")
	}

	// 4KB temporary stack buffer for draining non-blocking socket read queue
	buf := make([]byte, 4096)
	for {
		// unix.Read performs raw, non-blocking read system call on socket file descriptor (c.fd)
		n, err := unix.Read(c.fd, buf)
		if n > 0 {
			if n > c.maxInbound-c.inBuf.Len() {
				return fmt.Errorf("input buffer limit exceeded on fd %d", c.fd)
			}
			c.inBuf.Write(buf[:n]) // Append read bytes into inBuf
			c.lastActivity = time.Now()
			if err := c.processCommands(); err != nil {
				return err
			}
		}

		if err != nil {
			// EAGAIN / EWOULDBLOCK means all available bytes on non-blocking socket have been read.
			// This is normal non-blocking socket behavior, NOT an error!
			if errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EWOULDBLOCK) {
				break
			}
			// EINTR means read was interrupted by signal; retry.
			if errors.Is(err, unix.EINTR) {
				continue
			}
			// Fatal socket read error
			return fmt.Errorf("read error on fd %d: %w", c.fd, err)
		}

		if n == 0 {
			// In TCP sockets, unix.Read returning 0 bytes with no error indicates EOF (remote client closed socket).
			return io.EOF
		}
	}

	// Process any full RESP commands accumulated inside inBuf
	return c.processCommands()
}

// processCommands tries to decode and execute one or more RESP commands pending in c.inBuf.
//
// It handles partial TCP packets safely by leaving incomplete frames in the buffer until the next read.
// A successful decode advances the logical input cursor by exactly the number
// of bytes consumed, so pipelined commands are processed in order without
// recreating a reader for the remaining input.
func (c *Connection) processCommands() error {
	raw := c.inBuf.Bytes()
	reader := bytes.NewReader(raw)
	bufReader := bufio.NewReader(reader)
	decoder := resp.NewDecoderWithLimit(bufReader, c.maxInbound)
	consumed := 0
	consume := func() {
		c.inBuf.Next(consumed)
	}

	for consumed < len(raw) {
		value, err := decoder.Decode()
		if err != nil {
			// Detect partial TCP command payload (waiting for more network data)
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				// Not enough bytes for full RESP message yet; keep buffer intact and exit parsing loop
				break
			}

			// Malformed protocol error: send RESP Error response and reset input buffer
			respErr := resp.NewError(fmt.Sprintf("ERR protocol error: %v", err))
			if writeErr := c.writeResponse(respErr); writeErr != nil {
				consume()
				return writeErr
			}
			c.inBuf.Reset()
			return c.Flush()
		}

		// Calculate exact byte count consumed by decoder for this RESP value.
		//
		// WHY we subtract both reader.Len() AND bufReader.Buffered():
		//   bufio.NewReader pre-fetches bytes from the underlying `reader` into an internal
		//   4096-byte buffer eagerly. After Decode() finishes, `reader.Len()` reflects bytes
		//   NOT yet pulled into bufio's buffer, and `bufReader.Buffered()` reflects bytes
		//   pulled into bufio's buffer but NOT yet consumed by the decoder.
		//   Omitting bufReader.Buffered() causes consumed == len(raw) on every call,
		//   silently discarding all remaining pipelined commands in inBuf.
		//
		//   Correct formula:
		//     consumed = total_snapshot - bytes_never_read_into_bufio - bytes_in_bufio_but_not_decoded
		consumed = len(raw) - reader.Len() - bufReader.Buffered()

		// Step 1: Parse decoded RESP Value into structured Command (Name & Args)
		cmd, err := c.parser.Parse(value)
		if err != nil {
			respErr := resp.NewError(err.Error())
			if writeErr := c.writeResponse(respErr); writeErr != nil {
				consume()
				return writeErr
			}
			continue
		}

		// Step 2: Execute command (e.g., PING -> PONG)
		response, err := c.executor.Execute(cmd)
		if err != nil {
			respErr := resp.NewError(err.Error())
			if writeErr := c.writeResponse(respErr); writeErr != nil {
				consume()
				return writeErr
			}
			continue
		}

		// Step 3: Serialize response Value into bounded outbound buffer outBuf
		if err := c.writeResponse(response); err != nil {
			consume()
			return err
		}
	}

	consume()
	// Attempt to flush queued outbound response bytes to non-blocking socket
	return c.Flush()
}

// writeResponse serializes a RESP Value (e.g., SimpleString "+PONG\r\n") into outBuf.
func (c *Connection) writeResponse(val resp.Value) error {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&limitedBuffer{buffer: &buf, limit: c.maxOutbound - c.outBuf.Len()})
	encoder := resp.NewEncoder(writer)

	if err := encoder.Encode(val); err != nil {
		return fmt.Errorf("encode response: %w", err)
	}
	if err := writer.Flush(); err != nil {
		return fmt.Errorf("response buffer limit exceeded: %w", err)
	}
	if c.outBuf.Len() == 0 {
		c.writeStarted = time.Now()
	}
	_, err := c.outBuf.Write(buf.Bytes())
	return err
}

type limitedBuffer struct {
	buffer *bytes.Buffer
	limit  int
}

func (w *limitedBuffer) Write(p []byte) (int, error) {
	if len(p) > w.limit {
		return 0, fmt.Errorf("output limit exceeded")
	}
	return w.buffer.Write(p)
}

// Flush sends any queued response bytes to the client socket.
//
// If the socket becomes temporarily full, this method registers EPOLLOUT so the reactor can resume the
// write once the kernel reports the socket is writable again. The write loop continues until all queued
// bytes are drained or the socket is marked non-writable.
func (c *Connection) Flush() error {
	for c.outBuf.Len() > 0 {
		// Non-blocking write system call to client socket FD
		n, err := unix.Write(c.fd, c.outBuf.Bytes())
		if n > 0 {
			c.lastActivity = time.Now()
		}
		if n > 0 {
			c.outBuf.Next(n) // Drain successfully written bytes from outBuf
		}

		if err != nil {
			// EAGAIN / EWOULDBLOCK indicates OS socket send buffer is full!
			if errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EWOULDBLOCK) {
				// Register EPOLLOUT interest so Poller alerts us when socket becomes writable again
				return c.poller.Modify(c.fd, DefaultEventMask|unix.EPOLLOUT)
			}
			// EINTR means write syscall was interrupted by OS signal; retry
			if errors.Is(err, unix.EINTR) {
				continue
			}
			// Fatal socket write error
			return fmt.Errorf("write error on fd %d: %w", c.fd, err)
		}
		if n == 0 {
			return fmt.Errorf("write made no progress on fd %d", c.fd)
		}
	}

	// All queued bytes in outBuf successfully sent!
	c.writeStarted = time.Time{}
	// Reset interest mask back to default (disabling EPOLLOUT to prevent unnecessary CPU wakeups)
	return c.poller.Modify(c.fd, DefaultEventMask)
}

// OnWrite is triggered by EventLoop when epoll signals socket write readiness (EPOLLOUT).
// Invokes `Flush()` to send remaining queued outbound bytes in `outBuf`.
func (c *Connection) OnWrite() error {
	if c.isClosed {
		return errors.New("connection closed")
	}

	return c.Flush()
}

// RemoteAddr retrieves the peer IP address and port of the client connected to socket file descriptor c.fd.
// Uses system call `unix.Getpeername(c.fd)`.
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

// Close unregisters the connection file descriptor from epoll and closes the underlying OS socket handle.
func (c *Connection) Close() error {
	if c.isClosed {
		return nil // Idempotent close protection
	}
	c.isClosed = true

	// Step 1: Remove FD from epoll kernel interest tree
	_ = c.poller.Unregister(c.fd)

	// Step 2: Perform syscall to close OS socket file descriptor
	err := unix.Close(c.fd)
	if err != nil && !errors.Is(err, syscall.EBADF) {
		return fmt.Errorf("failed to close fd %d: %w", c.fd, err)
	}
	return nil
}
