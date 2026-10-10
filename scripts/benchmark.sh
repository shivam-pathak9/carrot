#!/usr/bin/env bash
set -euo pipefail

# These defaults make local runs comparable while allowing overrides for a
# machine with occupied ports or a larger benchmark budget.
BENCH_RUNS=${BENCH_RUNS:-3}
BENCH_REQUESTS=${BENCH_REQUESTS:-100000}
BENCH_CLIENTS=${BENCH_CLIENTS:-10}
BENCH_DATA_SIZE=${BENCH_DATA_SIZE:-16}
BENCH_PIPELINE=${BENCH_PIPELINE:-16}
STANDARD_PORT=${STANDARD_PORT:-16379}
REACTOR_PORT=${REACTOR_PORT:-16380}
REDIS_PORT=${REDIS_PORT:-16381}
BENCH_CPU_PROFILE_DIR=${BENCH_CPU_PROFILE_DIR:-}

for tool in go redis-cli redis-benchmark redis-server timeout; do
	if ! command -v "$tool" >/dev/null 2>&1; then
		echo "required tool not found: $tool" >&2
		exit 1
	fi
done

if ! [[ "$BENCH_RUNS" =~ ^[1-9][0-9]*$ && "$BENCH_REQUESTS" =~ ^[1-9][0-9]*$ &&
	"$BENCH_CLIENTS" =~ ^[1-9][0-9]*$ && "$BENCH_DATA_SIZE" =~ ^[1-9][0-9]*$ &&
	"$BENCH_PIPELINE" =~ ^[1-9][0-9]*$ && "$STANDARD_PORT" =~ ^[1-9][0-9]{0,4}$ &&
	"$REACTOR_PORT" =~ ^[1-9][0-9]{0,4}$ && "$REDIS_PORT" =~ ^[1-9][0-9]{0,4}$ ]] &&
	((STANDARD_PORT <= 65535 && REACTOR_PORT <= 65535 && REDIS_PORT <= 65535)) &&
	[[ "$STANDARD_PORT" != "$REACTOR_PORT" &&
		"$STANDARD_PORT" != "$REDIS_PORT" && "$REACTOR_PORT" != "$REDIS_PORT" ]]; then
	echo "benchmark counts and sizes must be positive integers; ports must be distinct values from 1 to 65535" >&2
	exit 1
fi

tmp_dir=$(mktemp -d)
server_pid=""

cleanup() {
	if [[ -n "$server_pid" ]] && kill -0 "$server_pid" 2>/dev/null; then
		kill -TERM "$server_pid" 2>/dev/null || true
		wait "$server_pid" 2>/dev/null || true
	fi
	rm -rf "$tmp_dir"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

go build -o "$tmp_dir/carrot-server" ./cmd/server
go build -o "$tmp_dir/carrot-reactor" ./cmd/reactor-server

echo "Date: $(date -Is)"
echo "Host: $(uname -a)"
echo "Go: $(go version)"
echo "redis-benchmark: $(redis-benchmark --version)"
echo "Redis server: $(redis-server --version)"
echo "Runs per server and pipeline depth: $BENCH_RUNS; requests: $BENCH_REQUESTS; clients: $BENCH_CLIENTS; payload bytes: $BENCH_DATA_SIZE"
if [[ "$BENCH_PIPELINE" == 1 ]]; then
	pipeline_depths=(1)
else
	pipeline_depths=(1 "$BENCH_PIPELINE")
fi
echo "Pipeline depths: ${pipeline_depths[*]}"
echo "AOF disabled for Carrot and Redis to compare network and in-memory command paths."
if [[ -n "$BENCH_CPU_PROFILE_DIR" ]]; then
	mkdir -p "$BENCH_CPU_PROFILE_DIR"
	echo "CPU profiles will be written to $BENCH_CPU_PROFILE_DIR (profile runs are diagnostic, not throughput measurements)."
fi

ensure_port_free() {
	local port=$1
	if timeout 1s redis-cli -h 127.0.0.1 -p "$port" ping >/dev/null 2>&1; then
		echo "port $port already has a Redis-compatible server; refusing to benchmark or stop it" >&2
		return 1
	fi
}

run_benchmarks() {
	local name=$1
	local port=$2

	for pipeline in "${pipeline_depths[@]}"; do
		for run in $(seq 1 "$BENCH_RUNS"); do
			echo
			echo "=== $name pipeline $pipeline run $run/$BENCH_RUNS ==="
			redis-benchmark --csv -h 127.0.0.1 -p "$port" \
				-n "$BENCH_REQUESTS" -c "$BENCH_CLIENTS" -d "$BENCH_DATA_SIZE" \
				-P "$pipeline" \
				-t ping_inline,ping_mbulk,set,get,lpush,rpush,lpop,rpop
		done
	done
}

wait_ready() {
	local name=$1
	local port=$2
	local log_file=$3
	local ready=false
	for _ in $(seq 1 50); do
		if timeout 1s redis-cli -h 127.0.0.1 -p "$port" ping 2>/dev/null | grep -qx PONG; then
			ready=true
			break
		fi
		if ! kill -0 "$server_pid" 2>/dev/null; then
			break
		fi
		sleep 0.1
	done
	if [[ "$ready" != true ]]; then
		echo "$name did not become ready; server output:" >&2
		sed -n '1,120p' "$log_file" >&2
		return 1
	fi
}

stop_server() {
	local name=$1
	local log_file=$2
	kill -TERM "$server_pid"
	local stopped_pid=$server_pid
	server_pid=""
	wait "$stopped_pid" || {
		echo "$name exited with an error during shutdown; server output:" >&2
		sed -n '1,120p' "$log_file" >&2
		return 1
	}
}

run_server() {
	local name=$1
	local binary=$2
	local port=$3
	local log_file="$tmp_dir/$name.log"
	local profile_args=()

	ensure_port_free "$port"
	if [[ -n "$BENCH_CPU_PROFILE_DIR" ]]; then
		profile_args=(-cpu-profile "$BENCH_CPU_PROFILE_DIR/$name.cpu.pprof")
	fi
	"$binary" -host 127.0.0.1 -port "$port" -aof-enabled=false "${profile_args[@]}" >"$log_file" 2>&1 &
	server_pid=$!

	wait_ready "$name" "$port" "$log_file"
	run_benchmarks "$name" "$port"
	stop_server "$name" "$log_file"
}

run_redis() {
	local log_file="$tmp_dir/redis.log"

	ensure_port_free "$REDIS_PORT"
	mkdir "$tmp_dir/redis-data"
	redis-server --bind 127.0.0.1 --port "$REDIS_PORT" \
		--save "" --appendonly no --dir "$tmp_dir/redis-data" \
		--loglevel warning >"$log_file" 2>&1 &
	server_pid=$!

	wait_ready Redis "$REDIS_PORT" "$log_file"
	run_benchmarks redis "$REDIS_PORT"
	stop_server Redis "$log_file"
}

run_server standard "$tmp_dir/carrot-server" "$STANDARD_PORT"
run_server reactor "$tmp_dir/carrot-reactor" "$REACTOR_PORT"
run_redis
