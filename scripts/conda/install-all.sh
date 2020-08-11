#!/usr/bin/env bash

set -ex
[ -z "$PREFIX" ] && export PREFIX=${CONDA_PREFIX:-/usr/local}

. ./build.sh

. ./install-omniscidb-common.sh
cmake --install build --component "exe" --prefix $PREFIX
cmake --install build --component "DBE" --prefix $PREFIX
. ./install-omniscidbe4py.sh
