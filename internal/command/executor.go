// executor.go dispatches parsed commands to their implementations.
package command

import (
	"fmt"
	"strings"
	"sync"

	"github.com/shivam-pathak9/carrot/internal/protocol/resp"
	"github.com/shivam-pathak9/carrot/internal/storage"
)

// Journal persists a command before the executor applies its mutation.
// Append may return a canonicalized command; the executor must apply that
// returned command so persisted absolute expirations match in-memory state.
type Journal interface {
	Append(Command) (Command, error)
}

// JournalRewriter is optionally implemented by journals that can compact
// their history from the current in-memory store.
type JournalRewriter interface {
	Rewrite(*storage.Store) error
}

// Executor is responsible for implementing the server-side behavior of supported commands.
// It holds a reference to the shared key-value storage engine.
type Executor struct {
	store   *storage.Store
	journal Journal
	writeMu sync.Mutex
}

// NewExecutor constructs a command executor backed by the provided storage engine.
func NewExecutor(store *storage.Store) *Executor {
	return &Executor{
		store: store,
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
		if len(cmd.Args) != 0 {
			return resp.NewError("ERR wrong number of arguments for 'aofrewrite' command"), nil
		}
		if err := e.RewriteAOF(); err != nil {
			return resp.Value{}, err
		}
		return resp.NewSimpleString("OK"), nil
	}

	if isMutation(cmd.Name) {
		// Hold the lock across journal append and mutation so concurrent writes
		// cannot be persisted in an order different from their in-memory order.
		e.writeMu.Lock()
		defer e.writeMu.Unlock()
		if e.journal != nil {
			persisted, err := e.journal.Append(cmd)
			if err != nil {
				return resp.Value{}, fmt.Errorf("persist command before applying it: %w", err)
			}
			cmd = persisted
		}
	}
	return e.execute(cmd)
}

// RewriteAOF blocks mutation commands while the journal replaces its history
// with a compact representation of the current store. Mutations that arrive
// during the rewrite wait on writeMu and are appended to the new file afterward.
// Journals without rewrite support return an explicit error.
func (e *Executor) RewriteAOF() error {
	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	if e.journal == nil {
		return fmt.Errorf("AOF rewrite is unavailable because persistence is disabled")
	}
	rewriter, ok := e.journal.(JournalRewriter)
	if !ok {
		return fmt.Errorf("configured journal does not support rewrite")
	}
	return rewriter.Rewrite(e.store)
}

// execute dispatches a command after Execute has applied persistence ordering.
// Keeping dispatch separate means replay can use the same command behavior
// through an executor that has no journal installed.
func (e *Executor) execute(cmd Command) (resp.Value, error) {
	switch cmd.Name {

	case "PING":
		switch len(cmd.Args) {
		case 0:
			return resp.NewSimpleString("PONG"), nil
		case 1:
			return resp.NewBulkString(cmd.Args[0]), nil
		default:
			return resp.NewError("ERR wrong number of arguments for 'ping' command"), nil
		}

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

// isMutation identifies commands that may change store state and therefore
// must pass through the journal boundary before execution.
func isMutation(name string) bool {
	switch name {
	case "SET", "DEL", "EXPIRE", "PEXPIREAT",
		"LPUSH", "RPUSH", "LPUSHX", "RPUSHX", "LPOP", "RPOP",
		"LSET", "LTRIM", "LREM", "LINSERT", "LMOVE", "RPOPLPUSH":
		return true
	default:
		return false
	}
}
