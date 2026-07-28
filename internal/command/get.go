package command

import (
	"github.com/shivampathak/carrot/internal/protocol/resp"
	"github.com/shivampathak/carrot/internal/storage"
)

// handleGet executes the GET command.
//
// Syntax: GET key
//
// Returns:
//   - Bulk String payload if key exists and has not expired.
//   - Null Bulk String (`$-1\r\n`) if key does not exist or is expired.
//   - Error if argument count is invalid.
func handleGet(store *storage.Store, args []string) (resp.Value, error) {
	if len(args) != 1 {
		return resp.NewError("ERR wrong number of arguments for 'get' command"), nil
	}

	key := args[0]
	val, found := store.Get(key)
	if !found {
		return resp.NewNullBulkString(), nil
	}

	return resp.NewBulkString(val), nil
}
