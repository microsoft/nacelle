#!/bin/bash
set -ex

RUSTFLAGS="--cfg loom -Dwarnings" cargo test -p nacelle-core --lib
