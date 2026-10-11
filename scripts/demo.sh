#!/usr/bin/env bash
# Walks through Carrot end to end against a real server: commands and TTLs,
# crash recovery from the AOF, and memory-limit eviction.
#
# Needs: go, redis-cli. Uses a temp directory and a loopback port, and cleans
# up after itself. Run it with `make demo`.
set -euo pipefail

cd "$(dirname "$0")/.."

PORT=${DEMO_PORT:-16399}
PAUSE=${DEMO_PAUSE:-0.5}

command -v redis-cli >/dev/null || { echo "redis-cli is required" >&2; exit 1; }

WORK=$(mktemp -d)
SERVER_PID=""
trap 'stop_server; rm -rf "$WORK"' EXIT

stop_server() {
	if [[ -n "$SERVER_PID" ]]; then
		kill "$SERVER_PID" 2>/dev/null || true
		wait "$SERVER_PID" 2>/dev/null || true
		SERVER_PID=""
	fi
}

start_server() {
	"$WORK/carrot" -host 127.0.0.1 -port "$PORT" "$@" >>"$WORK/server.log" 2>&1 &
	SERVER_PID=$!
	for _ in $(seq 1 50); do
		redis-cli -p "$PORT" PING >/dev/null 2>&1 && return
		sleep 0.1
	done
	echo "server did not start; log:" >&2
	cat "$WORK/server.log" >&2
	exit 1
}

act() { printf '\n\033[1;33m# %s\033[0m\n' "$*"; sleep "$PAUSE"; }
note() { printf '\033[2m  %s\033[0m\n' "$*"; }

# Prints the command like a shell prompt, then runs it.
cli() {
	printf '\033[1;32m$\033[0m redis-cli %s\n' "$*"
	redis-cli --no-raw -p "$PORT" "$@"
	sleep "$PAUSE"
}

quiet() { redis-cli -p "$PORT" "$@" >/dev/null; }

go build -o "$WORK/carrot" ./cmd/server

printf '\033[1m🥕 Carrot demo\033[0m\n'

act "1. Strings and expiry"
start_server -aof-file "$WORK/demo.aof" -aof-sync always
cli SET greeting "Hello, Carrot!"
cli GET greeting
cli SET session abc123 EX 2
cli TTL session
note "waiting for the key to expire..."
sleep 2.3
cli GET session

act "2. Lists"
cli RPUSH queue job-1 job-2 job-3
cli LPOP queue
cli LRANGE queue 0 -1

act "3. Crash recovery (kill -9, then restart from the AOF)"
cli SET survivor "written before the crash"
note "killing the server with SIGKILL..."
kill -9 "$SERVER_PID"
wait "$SERVER_PID" 2>/dev/null || true
SERVER_PID=""
sleep "$PAUSE"
start_server -aof-file "$WORK/demo.aof" -aof-sync always
note "restarted; replaying demo.aof"
cli GET survivor
cli GET greeting
cli LRANGE queue 0 -1
cli GET session
stop_server

act "4. Memory limit with allkeys-lru (8kb limit)"
start_server -aof-enabled=false -maxmemory 8kb -maxmemory-policy allkeys-lru
quiet SET hot-key "I keep getting read, so I stay"
value=$(printf 'x%.0s' $(seq 1 40))
note "writing 150 keys of 40 bytes; reading hot-key every 5 writes..."
for i in $(seq 1 150); do
	quiet SET "key-$i" "$value"
	((i % 5 == 0)) && quiet GET hot-key
done
cli GET hot-key
cli GET key-150
note "key-1 was the oldest, so it was evicted:"
cli GET key-1
stop_server

act "5. Memory limit with noeviction (8kb limit)"
start_server -aof-enabled=false -maxmemory 8kb -maxmemory-policy noeviction
accepted=0
while [[ $(redis-cli -p "$PORT" SET "key-$accepted" "$value") == "OK" ]]; do
	accepted=$((accepted + 1))
done
note "accepted $accepted writes, then:"
cli SET one-more "value"
cli GET key-0
cli DEL key-0
cli SET one-more "value"
stop_server

printf '\n\033[1mDone.\033[0m\n'
