#!/bin/sh
cd "$(dirname "$0")"
while true; do
  ./target/release/walker 2>>walker.log
  sleep 0.3
done
