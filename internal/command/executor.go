package command

import (
	"fmt"
	"strings"

	"github.com/shivampathak/carrot/internal/protocol/resp"
	"github.com/shivampathak/carrot/internal/storage"
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

// Execute runs the given Command and returns a RESP Value representing the response.
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

	default:
		return resp.NewError(
			fmt.Sprintf("ERR unknown command '%s'", strings.ToLower(cmd.Name)),
		), nil
	}
}
