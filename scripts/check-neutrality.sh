#!/usr/bin/env bash
# Neutrality gate (v0.7 SPEC §0.2 "no model code, no inference runtime, no embedding
# store"; v0.8 SPEC §4): scan the workspace's *normal* dependency graph and reject any
# model / inference / embedding / ML-runtime crate.
#
# Usage: bash scripts/check-neutrality.sh
# Note: set CARGO_TARGET_DIR externally if the workspace is on a FUSE mount.
set -euo pipefail
export PATH="$HOME/.cargo/bin:$PATH"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$(mktemp -d)/target}"
cd "$(dirname "$0")/.."

# Explicitly enumerated forbidden crate names (matched exactly against the first token of
# each `cargo tree --prefix none` line — exact matching means lookalike data-plane crates
# already in the tree, e.g. arrow2 / parquet2 / arrow-format / serde / bytemuck / snap,
# are NOT false positives).
FORBIDDEN=(
  # --- model formats / inference runtimes ---
  candle candle-core candle-nn candle-transformers candle-datasets candle-kernels
  candle-metal-kernels candle-flash-attn
  tch torch-sys
  onnx onnxruntime onnxruntime-sys ort ort-sys
  tract tract-core tract-hir tract-linalg tract-data tract-onnx tract-onnx-opl
  tract-nnef tract-tensorflow tract-pulse tract-metal
  tensorflow tensorflow-sys tensorflow-sys-packages
  burn burn-core burn-tensor burn-train burn-autodiff burn-dataset burn-import
  burn-onnx burn-tch burn-ndarray burn-wgpu burn-candle burn-fusion burn-jit
  ggml ggml-sys llama-cpp-2 llama-cpp-sys-2 mistralrs
  safetensors
  # --- tokenizers / model code ---
  tokenizers rust-bert pipelines
  # --- classical ML runtimes ---
  linfa smartcore xgboost xgboost-sys lightgbm lightgbm-sys catboost catboost-sys
  # --- embedding / vector stores ---
  hnsw hnsw_rs instant-distance usearch faiss faiss-sys qdrant-client
)

echo "== cargo tree --workspace -e normal =="
tree="$(cargo tree --workspace -e normal --prefix none)"
names="$(printf '%s\n' "$tree" | awk '{print $1}' | sort -u)"

fail=0
for crate in "${FORBIDDEN[@]}"; do
  if printf '%s\n' "$names" | grep -Fxq "$crate"; then
    echo "FORBIDDEN dependency detected: $crate" >&2
    printf '%s\n' "$tree" | grep -E "(^|[^a-zA-Z0-9_-])$crate " >&2 || true
    fail=1
  fi
done

if [ "$fail" -ne 0 ]; then
  echo "neutrality check FAILED: model/inference/embedding dependencies are not allowed" >&2
  exit 1
fi
echo "neutrality check: OK (no model/inference/embedding dependency in the workspace)"
