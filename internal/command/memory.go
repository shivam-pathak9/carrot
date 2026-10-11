// memory.go enforces the maxmemory limit before commands that add data.
package command

import (
	"fmt"

	"github.com/shivam-pathak9/carrot/internal/config"
	"github.com/shivam-pathak9/carrot/internal/protocol/resp"
)

const oomMessage = "OOM command not allowed when used memory > 'maxmemory'"

// SetMemoryLimit configures the memory limit and eviction policy. A limit of
// zero disables enforcement. Configure it before serving clients. Replay uses
// an executor without a limit, so recovery never evicts: evictions were
// journaled as DEL records and replay reproduces them.
func (e *Executor) SetMemoryLimit(maxBytes int64, policy string) {
	e.maxMemory = maxBytes
	e.evictLRU = policy == config.PolicyAllKeysLRU
}

// growsMemory reports whether a command can add data and so is refused or
// preceded by eviction when memory is full. Commands that only shrink or
// leave data unchanged (DEL, pops, LTRIM, LREM, EXPIRE) always run.
func growsMemory(name string) bool {
	switch name {
	case "SET", "LPUSH", "RPUSH", "LPUSHX", "RPUSHX", "LINSERT", "LSET", "LMOVE", "RPOPLPUSH":
		return true
	default:
		return false
	}
}

// admit decides whether cmd may run. It returns false when memory is full and
// nothing more can be freed, and an error if an eviction could not be
// persisted. Callers must hold mutationMu when a journal is installed.
func (e *Executor) admit(cmd Command) (bool, error) {
	if e.maxMemory <= 0 || !growsMemory(cmd.Name) {
		return true, nil
	}
	for e.store.UsedMemory() > e.maxMemory {
		if !e.evictLRU {
			return false, nil
		}
		key, found := e.store.LRUVictim()
		if !found {
			return false, nil
		}
		// Journal the DEL first so a failed write leaves the key in place.
		if e.journal != nil {
			del := Command{Name: "DEL", Args: []string{key}}
			if err := appendCommands(e.journal, []Command{del}); err != nil {
				return false, fmt.Errorf("persist eviction: %w", err)
			}
		}
		e.store.Del(key)
	}
	return true, nil
}

// admitOrReject runs admit and converts a refusal into the OOM reply.
func (e *Executor) admitOrReject(cmd Command) (rejected resp.Value, ok bool, err error) {
	ok, err = e.admit(cmd)
	if err != nil || ok {
		return resp.Value{}, ok, err
	}
	return resp.NewError(oomMessage), false, nil
}

// appendCommands persists commands through a single or batch journal.
func appendCommands(journal Journal, commands []Command) error {
	if batch, ok := journal.(BatchJournal); ok {
		_, err := batch.AppendBatch(commands)
		return err
	}
	for _, cmd := range commands {
		if _, err := journal.Append(cmd); err != nil {
			return err
		}
	}
	return nil
}
