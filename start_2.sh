#!/bin/bash

set -e

cargo build
RUST_BACKTRACE=1 JOSEFINE_ID=2 JOSEFINE_PORT=8082 ./target/debug/josefine --config Config.toml

