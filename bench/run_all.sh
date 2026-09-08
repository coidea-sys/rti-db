#!/bin/sh
# run_all.sh — rti-db 权威基准评测台一键复现脚本
# 用法: sh run_all.sh [--smoke]
# 全流程: 编译 → 启动 Redis/ClickHouse → rtidb → sqlite → redis → clickhouse → recovery → 停服务
# Results: bench/results/*.json
set -e
export PATH=$HOME/.cargo/bin:$PATH
export CARGO_TARGET_DIR=/root/target-bench

HARNESS="$(cd "$(dirname "$0")" && pwd)"
RESULTS="$HARNESS/results"
REDIS_BIN=/tmp/redis-7.2.5/src/redis-server
REDIS_CLI=/tmp/redis-7.2.5/src/redis-cli
CH_BIN=/tmp/ch/clickhouse-common-static-24.8.4.13/usr/bin/clickhouse
REDIS_PORT=6390
CH_PORT=8124
SMOKE=""
[ "$1" = "--smoke" ] && SMOKE="--smoke"

mkdir -p "$RESULTS" /tmp/bench
rm -f "$RESULTS"/*.json

echo "=== [0/6] 编译 bench-harness ==="
cd "$HARNESS"
cargo build --release 2>&1 | tail -1
BIN=$CARGO_TARGET_DIR/release

stop_services() {
    $REDIS_CLI -p $REDIS_PORT shutdown nosave 2>/dev/null || true
    pkill -f "clickhouse server" 2>/dev/null || true
}
trap stop_services EXIT

echo "=== [1/6] 启动 Redis (port $REDIS_PORT) ==="
rm -rf /tmp/bench/redis-data && mkdir -p /tmp/bench/redis-data
printf 'port %s\ndir /tmp/bench/redis-data\nappendonly no\nsave ""\nlogfile /tmp/bench/redis.log\ndaemonize yes\n' "$REDIS_PORT" > /tmp/bench/redis-6390.conf
$REDIS_BIN /tmp/bench/redis-6390.conf
for i in $(seq 1 50); do $REDIS_CLI -p $REDIS_PORT ping 2>/dev/null | grep -q PONG && break; sleep 0.2; done

echo "=== [2/6] 启动 ClickHouse (http $CH_PORT) ==="
rm -rf /tmp/ch-data && mkdir -p /tmp/ch-data
nohup $CH_BIN server -- --path=/tmp/ch-data --http_port=$CH_PORT --tcp_port=9194 > /tmp/bench/clickhouse.log 2>&1 &
for i in $(seq 1 100); do curl -s "http://127.0.0.1:$CH_PORT/ping" 2>/dev/null | grep -q Ok && break; sleep 0.5; done

echo "=== [3/6] rti-db ==="
$BIN/rtidb $SMOKE

echo "=== [4/6] sqlite ==="
$BIN/sqlite $SMOKE

echo "=== [5/6] redis ==="
$BIN/redis $SMOKE --port $REDIS_PORT

echo "=== [6/6] clickhouse + recovery ==="
$BIN/clickhouse $SMOKE --port $CH_PORT
$BIN/recovery run $SMOKE

echo "=== 写 env.md ==="
{
  echo "# env.md — 评测环境"
  echo
  echo "- date: $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  echo "- uname: $(uname -a)"
  echo "- cpu: $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2- | xargs) ($(nproc) cores)"
  echo "- mem: $(grep MemTotal /proc/meminfo | awk '{print $2/1048576 " GiB"}')"
  echo "- disk: $(df -h /tmp | tail -1 | awk '{print $1" "$2" (overlay2/容器卷)"}')"
  echo "- rustc: $(rustc --version)"
  echo "- rti-db: path dep /mnt/agents/output/project (main, v0.5)"
  echo "- redis: $($REDIS_BIN --version | sed 's/.*v=//;s/ .*//')"
  echo "- clickhouse: $($CH_BIN --version 2>/dev/null | head -1 | awk '{print $3}')"
  echo "- sqlite: rusqlite bundled $(grep -A1 'name = "libsqlite3-sys"' Cargo.lock | grep version | head -1 | cut -d'"' -f2)"
} > "$RESULTS/env.md"

stop_services
trap - EXIT
echo "=== DONE: $RESULTS ==="
ls -la "$RESULTS"
