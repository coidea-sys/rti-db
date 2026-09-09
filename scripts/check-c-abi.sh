#!/usr/bin/env bash
# Diff-check the frozen rti-vla C ABI header against cbindgen output.
set -euo pipefail

cd "$(dirname "$0")/.."

if ! command -v cbindgen >/dev/null 2>&1; then
  echo "cbindgen not found; install with: cargo install cbindgen --locked" >&2
  exit 127
fi

tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT
cbindgen --config cbindgen.toml --crate rti-vla --output "$tmp"
diff -u crates/rti-vla/include/rti_vla.h "$tmp"
echo "C ABI header check: OK"
