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

// Connection represents a non-blocking client TCP socket managed by the single-threaded Reactor EventLoop.
//
// Struct Fields & Why they are part of Connection:
//   - fd       : The OS file descriptor integer for this specific client socket (e.g. fd=5).
//                Used for non-blocking read (`unix.Read`) and write (`unix.Write`) operations.
//   - poller   : Pointer to shared Poller instance. Needed to dynamically modify epoll interest flags
//                (e.g., adding EPOLLOUT when outbound buffer cannot be flushed immediately).
//   - parser   : Pointer to shared command.Parser. Parses decoded RESP Values into executable Command objects.
//   - executor : Pointer to shared command.Executor. Executes commands (like PING) and returns RESP response Values.
//   - inBuf    : Memory byte buffer accumulating unparsed incoming bytes received from non-blocking socket reads.
//                Crucial for handling partial TCP frame delivery (TCP segmentation).
//   - outBuf   : Memory byte buffer queuing serialized RESP response bytes waiting to be sent to client socket.
//                Crucial for non-blocking socket writes when OS TCP socket send buffer is temporarily full.
//   - isClosed : Guard flag ensuring idempotent close operations and preventing operations on closed FDs.
type Connection struct {
	fd       int              // OS File descriptor representing client socket
	poller   *Poller          // Epoll handle to update event interest masks
	parser   *command.Parser  // Shared RESP -> Command parser
	executor *command.Executor// Shared Command -> RESP response executor

	inBuf  bytes.Buffer // Inbound stream buffer (accumulates partial TCP frames)
	outBuf bytes.Buffer // Outbound stream buffer (queues pending socket writes)

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
	return &Connection{
		fd:       fd,
		poller:   poller,
		parser:   parser,
		executor: executor,
	}
}

// FD returns the underlying numeric OS socket file descriptor.
func (c *Connection) FD() int {
	return c.fd
}

// OnRead is triggered by the EventLoop whenever epoll signals read readiness (EPOLLIN).
//
// Flow:
//   1. Performs non-blocking unix.Read() in a loop into a scratch buffer (`buf`).
//   2. Appends read bytes into `c.inBuf`.
//   3. Handles non-blocking system call return codes:
//      - `EAGAIN` / `EWOULDBLOCK`: Socket read buffer is currently drained. Break loop.
//      - `EINTR`: Interrupted by OS signal. Retry read.
//      - `n == 0`: Client closed connection gracefully (EOF). Return io.EOF.
//   4. Triggers `processCommands()` to parse and execute any complete RESP commands accumulated in `c.inBuf`.
func (c *Connection) OnRead() error {
	if c.isClosed {
		return errors.New("connection closed")
	}

	// 4KB temporary stack buffer for draining non-blocking socket read queue
	buf := make([]byte, 4096)
	for {
		// unix.Read performs raw, non-blocking read system call on socket file descriptor (c.fd)
		n, err := unix.Read(c.fd, buf)
		if n > 0 {
			c.inBuf.Write(buf[:n]) // Append read bytes into inBuf
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

// processCommands parses RESP protocol values from `c.inBuf`, executes them, and queues responses into `c.outBuf`.
//
// Partial Network Packet & Frame Accumulation Strategy:
//   - A single TCP read may contain partial commands (e.g. "*1\r\n$4\r\nPIN") or multiple combined commands.
//   - We peek into `c.inBuf` using a `bytes.Reader` snapshot.
//   - If `decoder.Decode()` encounters `io.EOF` or `io.ErrUnexpectedEOF`, it means the command payload is incomplete.
//     We leave unparsed bytes in `c.inBuf` and wait for the next EPOLLIN event to deliver remaining bytes!
//   - If decoding succeeds, we advance `c.inBuf` by the exact number of consumed bytes, parse command, execute, and write response.
func (c *Connection) processCommands() error {
	for c.inBuf.Len() > 0 {
		raw := c.inBuf.Bytes()                // Inspect current byte slice snapshot in inBuf
		reader := bytes.NewReader(raw)        // Create reader over snapshot
		bufReader := bufio.NewReader(reader)  // Wrap in bufio.Reader required by RESP Decoder
		decoder := resp.NewDecoder(bufReader) // Instantiate RESP protocol decoder

		// Attempt to decode next RESP Value
		value, err := decoder.Decode()
		if err != nil {
			// Detect partial TCP command payload (waiting for more network data)
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				// Not enough bytes for full RESP message yet; keep buffer intact and exit parsing loop
				break
			}

			// Malformed protocol error: send RESP Error response and reset input buffer
			respErr := resp.NewError(fmt.Sprintf("ERR protocol error: %v", err))
			c.writeResponse(respErr)
			c.inBuf.Reset()
			return c.Flush()
		}

		// Calculate exact byte count consumed by decoder for this RESP value:
		// len(raw) is initial snapshot length, reader.Len() is remaining unread bytes in reader.
		consumed := len(raw) - reader.Len()
		c.inBuf.Next(consumed) // Advance inBuf by consumed byte count

		// Step 1: Parse decoded RESP Value into structured Command (Name & Args)
		cmd, err := c.parser.Parse(value)
		if err != nil {
			respErr := resp.NewError(err.Error())
			c.writeResponse(respErr)
			continue
		}

		// Step 2: Execute command (e.g., PING -> PONG)
		response, err := c.executor.Execute(cmd)
		if err != nil {
			respErr := resp.NewError(err.Error())
			c.writeResponse(respErr)
			continue
		}

		// Step 3: Serialize response Value into outbound buffer outBuf
		c.writeResponse(response)
	}

	// Attempt to flush queued outbound response bytes to non-blocking socket
	return c.Flush()
}

// writeResponse serializes a RESP Value (e.g., SimpleString "+PONG\r\n") into outBuf.
func (c *Connection) writeResponse(val resp.Value) {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	encoder := resp.NewEncoder(writer)

	if err := encoder.Encode(val); err == nil {
		_ = writer.Flush()
		c.outBuf.Write(buf.Bytes()) // Queue serialized bytes into outBuf for transmission
	}
}

// Flush writes queued response bytes from `c.outBuf` to non-blocking client socket using unix.Write.
//
// Epoll Interest Handling for Non-Blocking Writes:
//   - Calls `unix.Write(c.fd, bytes)` in a loop.
//   - If all bytes are written (`c.outBuf.Len() == 0`):
//     Resets epoll event mask to `DefaultEventMask` (EPOLLIN | EPOLLERR | EPOLLRDHUP) — removing EPOLLOUT interest.
//   - If OS TCP socket send buffer is full (`err == EAGAIN` or `EWOULDBLOCK`):
//     Modifies epoll event mask to include `unix.EPOLLOUT`. When OS socket buffer has space,
//     epoll will wake up EventLoop, which calls `OnWrite()` to finish sending remaining bytes!
func (c *Connection) Flush() error {
	for c.outBuf.Len() > 0 {
		// Non-blocking write system call to client socket FD
		n, err := unix.Write(c.fd, c.outBuf.Bytes())
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
	}

	// All queued bytes in outBuf successfully sent!
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
