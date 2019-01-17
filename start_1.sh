#!/bin/bash

set -e

cargo build
RUST_BACKTRACE=1 JOSEFINE_ID=1 JOSEFINE_PORT=8081 ./target/debug/josefine --config Config.toml

