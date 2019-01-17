#!/bin/bash

set -e

cargo build

for i in $(seq 1 5); do
    RUST_BACKTRACE=1 JOSEFINE_ID="$i" JOSEFINE_PORT=$(python -c "print $i + 8080") ./target/debug/josefine --config Config.toml &
    sleep 3
done

wait
