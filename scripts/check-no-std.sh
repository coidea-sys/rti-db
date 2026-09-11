#!/usr/bin/env bash
# v0.3 no_std 冒烟检查（SPEC-evolution Wave 2 §2）。
# 用法：bash scripts/check-no-std.sh
# Note: set CARGO_TARGET_DIR externally if the workspace is on a FUSE mount.
set -euo pipefail
export PATH="$HOME/.cargo/bin:$PATH"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$(mktemp -d)/target}"
cd "$(dirname "$0")/.."

echo "== rti-core --no-default-features =="
cargo check -p rti-core --no-default-features
echo "== rti-mem --no-default-features =="
cargo check -p rti-mem --no-default-features
echo "== rti-buffer --no-default-features =="
cargo check -p rti-buffer --no-default-features
echo "== rti-query --no-default-features (v0.9) =="
cargo check -p rti-query --no-default-features
echo "no_std smoke check: OK"
