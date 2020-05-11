#!/bin/bash
set -e

$ARROW_ROOT/cpp/release/release/plasma-store-server -m 50000000 -s /tmp/plasma &
PLASMASERVER_PID=$!
./gradlew build --quiet ; kill $PLASMASERVER_PID

