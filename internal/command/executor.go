// executor.go dispatches parsed commands to their implementations.
package command

import (
	"fmt"
	"strings"

	"github.com/shivam-pathak9/carrot/internal/protocol/resp"
	"github.com/shivam-pathak9/carrot/internal/storage"
)

// Executor is responsible for implementing the server-side behavior of supported commands.
// It holds a reference to the shared key-value storage engine.
type Executor struct {
	store *storage.Store
}

// NewExecutor constructs a command executor backed by the provided storage engine.
func NewExecutor(store *storage.Store) *Executor {
	return &Executor{
		store: store,
	}
}

// Execute runs a parsed command and returns its RESP response. Command-level
// errors are represented as RESP error values; the Go error is reserved for
// failures that prevent command execution.
func (e *Executor) Execute(cmd Command) (resp.Value, error) {
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
