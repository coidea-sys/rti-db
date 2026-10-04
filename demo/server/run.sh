#!/bin/sh
# run.sh — robot-server 监督进程：崩溃（kill -9 / exit 9）后自动重启，
# 新进程打开同一个 Db 目录，触发真实 WAL 重放。
cd "$(dirname "$0")"
mkdir -p data
while true; do
  ./target/release/robot-server 2>>server.log
  echo "[supervisor] robot-server exited ($?), restarting in 0.3s" >>server.log
  sleep 0.3
done
