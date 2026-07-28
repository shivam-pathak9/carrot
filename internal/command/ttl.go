package command

import (
	"github.com/shivampathak/carrot/internal/protocol/resp"
	"github.com/shivampathak/carrot/internal/storage"
)

// handleTTL executes the TTL command.
//
// Syntax: TTL key
//
// Returns:
//   - Integer -2 if the key does not exist or has expired.
//   - Integer -1 if the key exists but has no associated expire.
//   - Integer TTL in seconds if the key exists and has an expiration set.
func handleTTL(store *storage.Store, args []string) (resp.Value, error) {
	if len(args) != 1 {
		return resp.NewError("ERR wrong number of arguments for 'ttl' command"), nil
	}

	key := args[0]
	ttlSeconds := store.TTL(key)
	return resp.NewInteger(ttlSeconds), nil
}
