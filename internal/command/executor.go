// executor.go dispatches parsed commands to their implementations.
package command

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/shivam-pathak9/carrot/internal/protocol/resp"
	"github.com/shivam-pathak9/carrot/internal/storage"
)

// Journal persists a command before the executor applies its mutation.
// Append may return a canonicalized command; the executor must apply that
// returned command so persisted absolute expirations match in-memory state.
type Journal interface {
	Append(Command) (Command, error)
}

// BatchJournal persists a group of commands before the executor applies any
// of them. AppendBatch returns canonicalized commands in input order.
type BatchJournal interface {
	AppendBatch([]Command) ([]Command, error)
	GroupCommitDelay() time.Duration
}

// JournalRewriter is optionally implemented by journals that can compact
// their history from the current in-memory store.
type JournalRewriter interface {
	Rewrite(*storage.Store) error
}

// AsyncJournalRewriter writes a point-in-time snapshot in the background,
// captures intervening mutations, and installs the replacement at completion.
type AsyncJournalRewriter interface {
	StartRewrite([]storage.SnapshotEntry) (<-chan error, error)
	CompleteRewrite() error
	AbortRewrite(error) error
}

// Executor is responsible for implementing the server-side behavior of supported commands.
// It holds a reference to the shared key-value storage engine.
type Executor struct {
	store         *storage.Store
	journal       Journal
	mutationMu    sync.Mutex
	rewriteMu     sync.RWMutex
	rewriteStatus string
	rewriteError  string
	dispatchMu    sync.Mutex
	dispatching   bool
	writeQueue    chan *executorRequest
}

type executorRequest struct {
	cmd          Command
	result       chan executionResult
	rewriteStart chan rewriteStartResult
}

type executionResult struct {
	value resp.Value
	err   error
}

type rewriteStartResult struct {
	done <-chan struct{}
	err  error
}

// NewExecutor constructs a command executor backed by the provided storage engine.
func NewExecutor(store *storage.Store) *Executor {
	return &Executor{
		store:      store,
		writeQueue: make(chan *executorRequest, 1024),
	}
}

// SetJournal installs or removes the persistence boundary. Configure it before
// serving clients; changing it while commands execute would break write ordering.
func (e *Executor) SetJournal(journal Journal) {
	e.journal = journal
}

// Execute runs a parsed command and returns its RESP response. Command-level
// errors are represented as RESP error values; the Go error is reserved for
// failures that prevent command execution.
func (e *Executor) Execute(cmd Command) (resp.Value, error) {
	if cmd.Name == "AOFREWRITE" {
		if len(cmd.Args) == 1 && strings.EqualFold(cmd.Args[0], "STATUS") {
			return e.aofRewriteStatus(), nil
		}
		if len(cmd.Args) != 0 {
			return resp.NewError("ERR wrong number of arguments for 'aofrewrite' command"), nil
		}
		if _, batched := e.journal.(BatchJournal); batched {
			if err := e.enqueueAOFRewrite(); err != nil {
				return resp.Value{}, err
			}
		} else if _, err := e.startAOFRewrite(); err != nil {
			return resp.Value{}, err
		}
		return resp.NewSimpleString("OK"), nil
	}

	if IsMutation(cmd.Name) && e.journal != nil {
		if _, ok := e.journal.(BatchJournal); ok {
			return e.submitMutation(cmd)
		}
		// With a journal installed, hold the lock across append and apply so
		// persisted order matches in-memory mutation order and rewrite snapshots.
		e.mutationMu.Lock()
		defer e.mutationMu.Unlock()
		persisted, err := e.journal.Append(cmd)
		if err != nil {
			return resp.Value{}, fmt.Errorf("persist command before applying it: %w", err)
		}
		cmd = persisted
	}
	return e.execute(cmd)
}

// RewriteAOF starts a rewrite and waits for it to finish. Network commands use
// the asynchronous AOFREWRITE command and query AOFREWRITE STATUS instead.
func (e *Executor) RewriteAOF() error {
	done, err := e.startAOFRewrite()
	if err != nil {
		return err
	}
	if done != nil {
		<-done
	}
	e.rewriteMu.RLock()
	defer e.rewriteMu.RUnlock()
	if e.rewriteStatus == "failed" {
		return fmt.Errorf("AOF rewrite failed: %s", e.rewriteError)
	}
	return nil
}

func (e *Executor) startAOFRewrite() (<-chan struct{}, error) {
	if _, ok := e.journal.(BatchJournal); ok {
		request := &executorRequest{rewriteStart: make(chan rewriteStartResult, 1)}
		e.enqueue(request)
		result := <-request.rewriteStart
		return result.done, result.err
	}
	return e.startAOFRewriteNow()
}

func (e *Executor) startAOFRewriteNow() (<-chan struct{}, error) {
	e.mutationMu.Lock()
	defer e.mutationMu.Unlock()
	if e.journal == nil {
		return nil, fmt.Errorf("AOF rewrite is unavailable because persistence is disabled")
	}
	e.rewriteMu.Lock()
	if e.rewriteStatus == "running" {
		e.rewriteMu.Unlock()
		return nil, fmt.Errorf("AOF rewrite is already in progress")
	}
	rewriter, ok := e.journal.(JournalRewriter)
	if !ok {
		e.rewriteMu.Unlock()
		return nil, fmt.Errorf("configured journal does not support rewrite")
	}
	e.rewriteStatus = "running"
	e.rewriteError = ""
	e.rewriteMu.Unlock()
	if asyncRewriter, ok := e.journal.(AsyncJournalRewriter); ok {
		entries, err := e.store.SnapshotEntries()
		if err != nil {
			err = fmt.Errorf("snapshot store for AOF rewrite: %w", err)
			e.setRewriteResult("failed", err)
			return nil, err
		}
		ready, err := asyncRewriter.StartRewrite(entries)
		if err != nil {
			e.setRewriteResult("failed", err)
			return nil, err
		}
		done := make(chan struct{})
		go e.finishAOFRewrite(asyncRewriter, ready, done)
		return done, nil
	}
	if err := rewriter.Rewrite(e.store); err != nil {
		e.setRewriteResult("failed", err)
		return nil, err
	}
	e.setRewriteResult("completed", nil)
	return nil, nil
}

func (e *Executor) enqueueAOFRewrite() error {
	if e.journal == nil {
		return fmt.Errorf("AOF rewrite is unavailable because persistence is disabled")
	}
	if _, ok := e.journal.(JournalRewriter); !ok {
		return fmt.Errorf("configured journal does not support rewrite")
	}
	e.rewriteMu.Lock()
	if e.rewriteStatus == "running" || e.rewriteStatus == "queued" {
		e.rewriteMu.Unlock()
		return fmt.Errorf("AOF rewrite is already in progress")
	}
	e.rewriteStatus = "queued"
	e.rewriteError = ""
	e.rewriteMu.Unlock()

	request := &executorRequest{rewriteStart: make(chan rewriteStartResult, 1)}
	e.enqueue(request)
	go func() {
		result := <-request.rewriteStart
		if result.err != nil {
			e.setRewriteResult("failed", result.err)
		}
	}()
	return nil
}

func (e *Executor) finishAOFRewrite(rewriter AsyncJournalRewriter, ready <-chan error, done chan struct{}) {
	defer close(done)
	if err := <-ready; err != nil {
		err = rewriter.AbortRewrite(err)
		e.setRewriteResult("failed", err)
		return
	}
	e.mutationMu.Lock()
	err := rewriter.CompleteRewrite()
	e.mutationMu.Unlock()
	if err != nil {
		e.setRewriteResult("failed", err)
		return
	}
	e.setRewriteResult("completed", nil)
}

func (e *Executor) setRewriteResult(status string, err error) {
	e.rewriteMu.Lock()
	e.rewriteStatus = status
	if err != nil {
		e.rewriteError = err.Error()
	} else {
		e.rewriteError = ""
	}
	e.rewriteMu.Unlock()
}

func (e *Executor) submitMutation(cmd Command) (resp.Value, error) {
	request := &executorRequest{cmd: cmd, result: make(chan executionResult, 1)}
	e.enqueue(request)
	result := <-request.result
	return result.value, result.err
}

func (e *Executor) enqueue(request *executorRequest) {
	e.dispatchMu.Lock()
	if !e.dispatching {
		e.dispatching = true
		go e.writeLoop()
	}
	e.writeQueue <- request
	e.dispatchMu.Unlock()
}

func (e *Executor) writeLoop() {
	var pending *executorRequest
	idleTimer := time.NewTimer(time.Second)
	defer idleTimer.Stop()
	for {
		request := pending
		pending = nil
		if request == nil {
			select {
			case request = <-e.writeQueue:
				if !idleTimer.Stop() {
					select {
					case <-idleTimer.C:
					default:
					}
				}
				idleTimer.Reset(time.Second)
			case <-idleTimer.C:
				e.dispatchMu.Lock()
				if len(e.writeQueue) == 0 {
					e.dispatching = false
					e.dispatchMu.Unlock()
					return
				}
				e.dispatchMu.Unlock()
				idleTimer.Reset(time.Second)
				continue
			}
		}
		if request.rewriteStart != nil {
			done, err := e.startAOFRewriteNow()
			request.rewriteStart <- rewriteStartResult{done: done, err: err}
			continue
		}

		batch := []*executorRequest{request}
		journal := e.journal.(BatchJournal)
		delay := journal.GroupCommitDelay()
		var timer *time.Timer
		var timerC <-chan time.Time
		if delay > 0 {
			timer = time.NewTimer(delay)
			timerC = timer.C
		}
		for len(batch) < 64 {
			select {
			case next := <-e.writeQueue:
				if next.rewriteStart != nil {
					pending = next
					goto commit
				}
				batch = append(batch, next)
			case <-timerC:
				goto commit
			default:
				if delay == 0 {
					goto commit
				}
				select {
				case next := <-e.writeQueue:
					if next.rewriteStart != nil {
						pending = next
						goto commit
					}
					batch = append(batch, next)
				case <-timerC:
					goto commit
				}
			}
		}
	commit:
		if timer != nil {
			timer.Stop()
		}
		e.commitMutationBatch(journal, batch)
	}
}

func (e *Executor) commitMutationBatch(journal BatchJournal, batch []*executorRequest) {
	commands := make([]Command, len(batch))
	for i, request := range batch {
		commands[i] = request.cmd
	}
	e.mutationMu.Lock()
	persisted, err := journal.AppendBatch(commands)
	if err == nil && len(persisted) != len(batch) {
		err = fmt.Errorf("journal returned %d commands for batch of %d", len(persisted), len(batch))
	}
	results := make([]executionResult, len(batch))
	for i := range batch {
		if err != nil {
			results[i].err = fmt.Errorf("persist command before applying it: %w", err)
			continue
		}
		results[i].value, results[i].err = e.execute(persisted[i])
	}
	e.mutationMu.Unlock()
	for i, request := range batch {
		request.result <- results[i]
	}
}

func (e *Executor) aofRewriteStatus() resp.Value {
	e.rewriteMu.RLock()
	defer e.rewriteMu.RUnlock()
	status := e.rewriteStatus
	if status == "" {
		status = "idle"
	}
	return resp.NewArray(resp.NewBulkString(status), resp.NewBulkString(e.rewriteError))
}

// execute dispatches a command after Execute has applied persistence ordering.
// Keeping dispatch separate means replay can use the same command behavior
// through an executor that has no journal installed.
func (e *Executor) execute(cmd Command) (resp.Value, error) {
	switch cmd.Name {

	case "PING":
		return handlePing(cmd.Args), nil

	case "GET":
		return handleGet(e.store, cmd.Args)

	case "SET":
		return handleSet(e.store, cmd.Args)

	case "TTL":
		return handleTTL(e.store, cmd.Args)

	case "DEL":
		return handleDel(e.store, cmd.Args)

	case "EXPIRE":
		return handleExpire(e.store, cmd.Args)

	case "PEXPIREAT":
		return handleExpireAt(e.store, cmd.Args)

	case "LPUSH", "RPUSH", "LPUSHX", "RPUSHX",
		"LPOP", "RPOP", "LLEN", "LRANGE", "LINDEX", "LSET", "LTRIM",
		"LREM", "LINSERT", "LMOVE", "RPOPLPUSH", "LPOS":
		return handleListCommand(cmd.Name, e.store, cmd.Args)

	default:
		return resp.NewError(
			fmt.Sprintf("ERR unknown command '%s'", strings.ToLower(cmd.Name)),
		), nil
	}
}

// IsMutation reports whether a command can modify the store and must pass
// through persistence ordering before execution or AOF replay.
func IsMutation(name string) bool {
	switch name {
	case "SET", "DEL", "EXPIRE", "PEXPIREAT",
		"LPUSH", "RPUSH", "LPUSHX", "RPUSHX", "LPOP", "RPOP",
		"LSET", "LTRIM", "LREM", "LINSERT", "LMOVE", "RPOPLPUSH":
		return true
	default:
		return false
	}
}
