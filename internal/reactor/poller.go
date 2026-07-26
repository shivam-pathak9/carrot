//go:build linux

package reactor

import (
	"errors"
	"fmt"

	"golang.org/x/sys/unix"
)

const (
	// DefaultMaxEvents defines the maximum number of events returned by a
	// single epoll_wait() call.
	DefaultMaxEvents = 128

	// DefaultEventMask represents the events we are interested in for every
	// client connection.
	DefaultEventMask = unix.EPOLLIN |
		unix.EPOLLERR |
		unix.EPOLLRDHUP
)

// Event represents a ready file descriptor returned by epoll.
type Event struct {
	FD     int
	Events uint32
}

// Poller is a thin wrapper around Linux epoll.
//
// Responsibilities:
//   - Create an epoll instance
//   - Register file descriptors
//   - Modify file descriptor interests
//   - Remove file descriptors
//   - Wait for ready events
//
// It intentionally knows nothing about TCP, RESP, Connections or Redis.
type Poller struct {
	epfd      int
	maxEvents int
}

// NewPoller creates a new epoll instance.
func NewPoller(maxEvents int) (*Poller, error) {
	if maxEvents <= 0 {
		maxEvents = DefaultMaxEvents
	}

	epfd, err := unix.EpollCreate1(0)
	if err != nil {
		return nil, fmt.Errorf("failed to create epoll instance: %w", err)
	}

	return &Poller{
		epfd:      epfd,
		maxEvents: maxEvents,
	}, nil
}

// Register registers a file descriptor using the default event mask.
func (p *Poller) Register(fd int) error {
	return p.RegisterWithEvents(fd, DefaultEventMask)
}

// RegisterWithEvents registers a file descriptor with a custom event mask.
func (p *Poller) RegisterWithEvents(fd int, events uint32) error {
	event := &unix.EpollEvent{
		Events: events,
		Fd:     int32(fd),
	}

	if err := unix.EpollCtl(
		p.epfd,
		unix.EPOLL_CTL_ADD,
		fd,
		event,
	); err != nil {
		return fmt.Errorf("failed to register fd %d: %w", fd, err)
	}

	return nil
}

// Modify changes the events being monitored for an already registered
// file descriptor.
func (p *Poller) Modify(fd int, events uint32) error {
	event := &unix.EpollEvent{
		Events: events,
		Fd:     int32(fd),
	}

	if err := unix.EpollCtl(
		p.epfd,
		unix.EPOLL_CTL_MOD,
		fd,
		event,
	); err != nil {
		return fmt.Errorf("failed to modify fd %d: %w", fd, err)
	}

	return nil
}

// Unregister removes a file descriptor from epoll.
func (p *Poller) Unregister(fd int) error {
	if err := unix.EpollCtl(
		p.epfd,
		unix.EPOLL_CTL_DEL,
		fd,
		nil,
	); err != nil {
		return fmt.Errorf("failed to unregister fd %d: %w", fd, err)
	}

	return nil
}

// Wait blocks until one or more file descriptors become ready.
//
// It automatically retries if interrupted by a signal (EINTR).
func (p *Poller) Wait() ([]Event, error) {
	events := make([]unix.EpollEvent, p.maxEvents)

	for {
		n, err := unix.EpollWait(
			p.epfd,
			events,
			-1,
		)

		if err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}

			return nil, fmt.Errorf("epoll_wait failed: %w", err)
		}

		ready := make([]Event, 0, n)

		for i := 0; i < n; i++ {
			ready = append(ready, Event{
				FD:     int(events[i].Fd),
				Events: events[i].Events,
			})
		}

		return ready, nil
	}
}

// Close closes the epoll instance.
func (p *Poller) Close() error {
	if err := unix.Close(p.epfd); err != nil {
		return fmt.Errorf("failed to close epoll fd: %w", err)
	}

	return nil
}
