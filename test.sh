#!/usr/bin/env bash

set -eo pipefail

# where am i?
me="$0"
me_home=$(dirname "$0")
me_home=$(cd "$me_home" && pwd)

# deps
DOCKERIZE=dockerize
COMPOSE=docker-compose
DRIVER=go

# environment
export GOBLOB_FS_ROOT="${me_home}/test/data"
export GOBLOB_FIXTURES="${me_home}/test/fixtures"
export STORAGE_EMULATOR_HOST=localhost:59022

# parse arguments
args=$(getopt dcv $*)
set -- $args
for i; do
  case "$i"
  in
    -d)
      debug="true";
      DRIVER="dlv"
      shift;;
    -c)
      other_flags="$other_flags -cover";
      shift;;
    -v)
      other_flags="$other_flags -v";
      shift;;
    --)
      shift; break;;
  esac
done

if [ ! -d "$GOBLOB_FS_ROOT" ]; then
  echo "==> $GOBLOB_FS_ROOT"
  mkdir -p "$GOBLOB_FS_ROOT"
fi

$COMPOSE -f "$me_home/test/services.compose" up -d
$DOCKERIZE -wait tcp://localhost:59022/ -timeout 30s
$DRIVER test$other_flags $*
$COMPOSE -f "$me_home/test/services.compose" down
