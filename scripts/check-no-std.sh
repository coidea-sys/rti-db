#!/usr/bin/env bash
# v0.3 no_std 冒烟检查（SPEC-evolution Wave 2 §2）。
# 用法：bash scripts/check-no-std.sh
# 注意：/mnt/agents 为 FUSE 挂载，cargo 必须指定本地 CARGO_TARGET_DIR。
set -euo pipefail
export PATH="$HOME/.cargo/bin:$PATH"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/root/target-v03}"
cd "$(dirname "$0")/.."

echo "== rti-core --no-default-features =="
cargo check -p rti-core --no-default-features
echo "== rti-mem --no-default-features =="
cargo check -p rti-mem --no-default-features
echo "== rti-buffer --no-default-features =="
cargo check -p rti-buffer --no-default-features
echo "no_std smoke check: OK"
