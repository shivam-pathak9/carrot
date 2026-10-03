package command

import (
	"strconv"
	"strings"
	"time"

	"github.com/shivam-pathak9/carrot/internal/protocol/resp"
	"github.com/shivam-pathak9/carrot/internal/storage"
)

// handleSet executes the SET command.
//
// Syntax: SET key value [EX seconds] [PX milliseconds] [PXAT unix-milliseconds]
//
// Supported Options:
//   - EX seconds     : Set the specified expire time, in seconds.
//   - PX milliseconds: Set the specified expire time, in milliseconds.
//
// Returns:
//   - Simple String "+OK" on success.
//   - Error on syntax errors or invalid expiration values.
func handleSet(store *storage.Store, args []string) (resp.Value, error) {
	if len(args) < 2 {
		return resp.NewError("ERR wrong number of arguments for 'set' command"), nil
	}

	key := args[0]
	val := args[1]
	var ttl time.Duration
	var expiresAt time.Time
	hasAbsoluteExpiry := false

	// Parse optional relative or absolute expiration arguments.
	i := 2
	for i < len(args) {
		opt := strings.ToUpper(args[i])
		switch opt {
		case "EX":
			if i+1 >= len(args) {
				return resp.NewError("ERR syntax error"), nil
			}
			sec, err := strconv.ParseInt(args[i+1], 10, 64)
			if err != nil || sec <= 0 || sec > int64((1<<63-1)/int64(time.Second)) {
				return resp.NewError("ERR invalid expire time in 'set' command"), nil
			}
			ttl = time.Duration(sec) * time.Second
			expiresAt = time.Time{}
			hasAbsoluteExpiry = false
			i += 2

		case "PX":
			if i+1 >= len(args) {
				return resp.NewError("ERR syntax error"), nil
			}
			ms, err := strconv.ParseInt(args[i+1], 10, 64)
			if err != nil || ms <= 0 || ms > int64((1<<63-1)/int64(time.Millisecond)) {
				return resp.NewError("ERR invalid expire time in 'set' command"), nil
			}
			ttl = time.Duration(ms) * time.Millisecond
			expiresAt = time.Time{}
			hasAbsoluteExpiry = false
			i += 2
		case "PXAT":
			if i+1 >= len(args) {
				return resp.NewError("ERR syntax error"), nil
			}
			milliseconds, err := strconv.ParseInt(args[i+1], 10, 64)
			if err != nil {
				return resp.NewError("ERR invalid expire time in 'set' command"), nil
			}
			expiresAt = time.UnixMilli(milliseconds)
			ttl = 0
			hasAbsoluteExpiry = true
			i += 2

		default:
			return resp.NewError("ERR syntax error"), nil
		}
	}

	if hasAbsoluteExpiry {
		store.SetAt(key, val, expiresAt)
	} else {
		store.Set(key, val, ttl)
	}
	return resp.NewSimpleString("OK"), nil
}
