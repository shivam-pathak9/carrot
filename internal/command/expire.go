package command

import (
	"strconv"

	"github.com/shivampathak/carrot/internal/protocol/resp"
	"github.com/shivampathak/carrot/internal/storage"
)

// handleExpire executes the EXPIRE command.
//
// Syntax: EXPIRE key seconds
//
// Sets a timeout on key. After the timeout has expired, the key will
// automatically be deleted.
//
// Returns:
//   - Integer 1 if the timeout was set successfully (or key deleted due to non-positive seconds).
//   - Integer 0 if the key does not exist or has expired.
//   - Error on invalid argument count or non-integer seconds value.
func handleExpire(store *storage.Store, args []string) (resp.Value, error) {
	if len(args) != 2 {
		return resp.NewError("ERR wrong number of arguments for 'expire' command"), nil
	}

	key := args[0]
	seconds, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return resp.NewError("ERR value is not an integer or out of range"), nil
	}

	ok := store.Expire(key, seconds)
	if !ok {
		// Key does not exist or was already expired
		return resp.NewInteger(0), nil
	}

	// Timeout was set successfully (or key deleted for <= 0 seconds)
	return resp.NewInteger(1), nil
}
