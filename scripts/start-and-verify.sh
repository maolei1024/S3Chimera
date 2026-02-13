#!/bin/bash
set -e

# Build jar if not present? No, CI should provide it.
JAR_PATH=$1

if [ -z "$JAR_PATH" ]; then
    echo "Usage: $0 <path-to-server-jar>"
    exit 1
fi

echo "Starting S3Chimera server..."
echo "Driver config: $CHIMERA_DRIVER_TYPE"

# Start server in background
# Pass through all environment variables from CI
nohup java -jar $JAR_PATH > server.log 2>&1 &
PID=$!

echo "Server PID: $PID"

# Wait for port 8080
echo "Waiting for S3 service on port 8080..."
MAX_RETRIES=60
for i in $(seq 1 $MAX_RETRIES); do
    if nc -z 127.0.0.1 8080; then
        echo "Server is up!"
        break
    fi
    if [ $i -eq $MAX_RETRIES ]; then
        echo "Server failed to start within ${MAX_RETRIES}s."
        cat server.log
        kill $PID || true
        exit 1
    fi
    sleep 1
done

# Run verification
echo "Running verification script..."
# Use correct relative path assuming running from project root
chmod +x scripts/verify-compatibility.sh
./scripts/verify-compatibility.sh 2>&1 | tee verify.log || TEST_EXIT_CODE=$?

echo "Stopping server..."
kill $PID || true

if [ -n "$TEST_EXIT_CODE" ]; then
    echo "Verification failed!"
    echo "=== Server Logs (Tail 100) ==="
    tail -n 100 server.log
    exit $TEST_EXIT_CODE
else
    echo "Verification success!"
fi
