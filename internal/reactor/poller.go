//go:build linux

package reactor

import (
	"errors"
	"fmt"

	"golang.org/x/sys/unix"
)

const (
	// DefaultMaxEvents defines the maximum number of ready file descriptor events
	// that a single unix.EpollWait() system call will return in one batch.
	//
	// Why 128? It balances memory allocation size for ready event slices against
	// batch throughput in event-driven systems (similar to libuv/Redis default batch sizes).
	DefaultMaxEvents = 128

	// DefaultEventMask represents the bitmask of interest events we monitor by default
	// for every registered file descriptor (both listener socket and client connection sockets).
	//
	// Breakdown of constants:
	//   - unix.EPOLLIN   : Notifies when data is available to read on the file descriptor.
	//   - unix.EPOLLERR  : Notifies when an error condition occurs on the file descriptor (e.g. pipe broken).
	//   - unix.EPOLLRDHUP: Notifies when the remote peer closed the connection or shut down writing half of connection (EOF detection at epoll level).
	DefaultEventMask = unix.EPOLLIN |
		unix.EPOLLERR |
		unix.EPOLLRDHUP
)

// Event represents a single ready file descriptor returned by epoll_wait.
//
// Fields:
//   - FD    : The numeric OS file descriptor (e.g. 3 for listener socket, 5, 6 for client sockets) that is ready for I/O.
//   - Events: Bitmask of triggered events (e.g., EPOLLIN, EPOLLOUT, EPOLLERR, EPOLLRDHUP).
type Event struct {
	FD     int    // OS File Descriptor number
	Events uint32 // Epoll event bitmask flags triggered for this FD
}

// Poller is a low-level, direct wrapper around Linux epoll system calls.
//
// Struct Fields & Why they exist:
//   - epfd     : The Linux Epoll instance file descriptor returned by EpollCreate1().
//                All epoll control operations (ADD, MOD, DEL, WAIT) operate on this handle.
//   - maxEvents: Maximum number of events to allocate memory for and retrieve per epoll_wait call.
type Poller struct {
	epfd      int // File descriptor referencing the kernel epoll interest tree
	maxEvents int // Maximum capacity of events slice passed to epoll_wait
}

// NewPoller creates and initializes a new Linux epoll instance using system call unix.EpollCreate1.
//
// Parameters:
//   - maxEvents: Upper limit of events retrieved per wait call (defaults to DefaultMaxEvents if <= 0).
func NewPoller(maxEvents int) (*Poller, error) {
	if maxEvents <= 0 {
		maxEvents = DefaultMaxEvents
	}

	// unix.EpollCreate1(0) creates an epoll object in the Linux kernel and returns its file descriptor (epfd).
	// Passing flag 0 behaves identically to legacy epoll_create without size constraints.
	epfd, err := unix.EpollCreate1(0)
	if err != nil {
		return nil, fmt.Errorf("failed to create epoll instance: %w", err)
	}

	return &Poller{
		epfd:      epfd,      // Store the epoll control FD handle
		maxEvents: maxEvents, // Store max event batch size
	}, nil
}

// Register adds a target file descriptor (fd) to the kernel epoll interest list using DefaultEventMask.
func (p *Poller) Register(fd int) error {
	return p.RegisterWithEvents(fd, DefaultEventMask)
}

// RegisterWithEvents registers a file descriptor (fd) into the epoll interest tree with a specific event mask.
//
// System Call: unix.EpollCtl(epfd, EPOLL_CTL_ADD, target_fd, &epoll_event)
//
// How it works:
//   1. Construct unix.EpollEvent with interest bitmask (e.g. EPOLLIN) and target FD.
//   2. Call unix.EpollCtl with command unix.EPOLL_CTL_ADD to insert 'fd' into kernel's interest tree.
func (p *Poller) RegisterWithEvents(fd int, events uint32) error {
	event := &unix.EpollEvent{
		Events: events,     // Bitmask of events we want kernel to monitor (EPOLLIN, etc.)
		Fd:     int32(fd),  // Target file descriptor to track
	}

	if err := unix.EpollCtl(
		p.epfd,             // Epoll instance file descriptor handle
		unix.EPOLL_CTL_ADD, // Command: ADD new file descriptor to interest list
		fd,                 // File descriptor being registered
		event,              // Pointer to epoll_event struct containing target FD & events
	); err != nil {
		return fmt.Errorf("failed to register fd %d: %w", fd, err)
	}

	return nil
}

// Modify updates the event interest bitmask for a file descriptor (fd) already registered in epoll.
//
// System Call: unix.EpollCtl(epfd, EPOLL_CTL_MOD, target_fd, &epoll_event)
//
// Example Usage: Adding unix.EPOLLOUT when client socket write buffer fills up, or removing it once flushed.
func (p *Poller) Modify(fd int, events uint32) error {
	event := &unix.EpollEvent{
		Events: events,    // New desired event bitmask (e.g. EPOLLIN | EPOLLOUT)
		Fd:     int32(fd), // Target file descriptor being modified
	}

	if err := unix.EpollCtl(
		p.epfd,             // Epoll instance file descriptor handle
		unix.EPOLL_CTL_MOD, // Command: MODIFY existing file descriptor interest mask
		fd,                 // Target file descriptor being modified
		event,              // Pointer to updated epoll_event struct
	); err != nil {
		return fmt.Errorf("failed to modify fd %d: %w", fd, err)
	}

	return nil
}

// Unregister removes a file descriptor (fd) completely from the kernel epoll interest tree.
//
// System Call: unix.EpollCtl(epfd, EPOLL_CTL_DEL, target_fd, nil)
//
// When to call: When a client connection is disconnected or closed to prevent kernel notification overhead.
func (p *Poller) Unregister(fd int) error {
	if err := unix.EpollCtl(
		p.epfd,             // Epoll instance file descriptor handle
		unix.EPOLL_CTL_DEL, // Command: DELETE file descriptor from interest list
		fd,                 // File descriptor being removed
		nil,                // Event struct pointer is ignored for EPOLL_CTL_DEL in modern Linux kernel
	); err != nil {
		return fmt.Errorf("failed to unregister fd %d: %w", fd, err)
	}

	return nil
}

// Wait blocks execution until one or more file descriptors registered in epoll become ready for I/O.
//
// System Call: unix.EpollWait(epfd, events_slice, timeout=-1)
//
// Parameters:
//   - timeout = -1 : Wait indefinitely until at least one event triggers.
//
// Signal Handling:
//   - If interrupted by an OS signal (EINTR), it automatically retries instead of crashing.
func (p *Poller) Wait() ([]Event, error) {
	// Pre-allocate slice of EpollEvent structures to receive ready events from kernel
	events := make([]unix.EpollEvent, p.maxEvents)

	for {
		// epoll_wait populates 'events' slice and returns 'n' (number of ready FDs)
		n, err := unix.EpollWait(
			p.epfd,   // Epoll handle
			events,   // Buffer slice to populate with ready events
			-1,       // Timeout in ms (-1 means block infinitely until event occurs)
		)

		if err != nil {
			// EINTR means the system call was interrupted by a kernel signal (e.g. SIGINT/SIGTERM)
			// It is not a fatal failure; retry wait loop.
			if errors.Is(err, unix.EINTR) {
				continue
			}

			return nil, fmt.Errorf("epoll_wait failed: %w", err)
		}

		// Map raw unix.EpollEvent items returned by kernel into clean reactor.Event structs
		ready := make([]Event, 0, n)
		for i := 0; i < n; i++ {
			ready = append(ready, Event{
				FD:     int(events[i].Fd),     // The ready socket/listener FD
				Events: events[i].Events,     // Bitmask of ready events (EPOLLIN, EPOLLOUT, etc.)
			})
		}

		return ready, nil
	}
}

// Close closes the epoll file descriptor handle, freeing Linux kernel resources.
func (p *Poller) Close() error {
	if err := unix.Close(p.epfd); err != nil {
		return fmt.Errorf("failed to close epoll fd: %w", err)
	}

	return nil
}
