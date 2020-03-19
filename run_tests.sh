plasma-store-server -m 1000000 -s /tmp/plasma &
PLASMASERVER_PID=$!
./gradlew build
kill $PLASMASERVER_PID
