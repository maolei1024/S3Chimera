#!/bin/bash
# Enhanced AWS CLI Blackbox Testing - Metadata Operations
# Tests: User metadata, Content-Type verification
set -e

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [METADATA] $*"
}

log_success() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [METADATA] ✅ $*"
}

log_error() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [METADATA] ❌ $*" >&2
}

run_metadata_tests() {
    local ENDPOINT=$1
    local BUCKET=$2
    
    log "Starting Metadata Tests"
    log "Endpoint: $ENDPOINT"
    log "Bucket: $BUCKET"
    
    local TEST_FILE="metadata-test.txt"
    
    # Setup: Create bucket and file
    log "Setup: Creating bucket and test file..."
    aws s3 mb s3://$BUCKET --endpoint-url "$ENDPOINT" 2>&1 || true
    echo "Metadata test content" > $TEST_FILE
    
    # Test 1: Upload with user metadata
    log "TEST 1: Upload with user metadata..."
    aws s3 cp $TEST_FILE s3://$BUCKET/with-metadata.txt \
        --endpoint-url "$ENDPOINT" \
        --metadata "author=testuser,version=1.0,environment=ci" 2>&1 || {
        log_error "Upload with metadata failed"
        return 1
    }
    log_success "Upload with metadata completed"
    
    # Test 2: Verify metadata via HeadObject
    log "TEST 2: Verify metadata via HeadObject..."
    local HEAD_RESULT=$(aws s3api head-object \
        --bucket $BUCKET \
        --key with-metadata.txt \
        --endpoint-url "$ENDPOINT" 2>&1)
    log "HeadObject response: $HEAD_RESULT"
    
    if echo "$HEAD_RESULT" | grep -qi "author"; then
        log_success "User metadata 'author' found!"
    else
        log_error "User metadata 'author' not found!"
        return 1
    fi
    
    # Test 3: Upload with custom Content-Type
    log "TEST 3: Upload with custom Content-Type..."
    aws s3 cp $TEST_FILE s3://$BUCKET/custom-type.json \
        --endpoint-url "$ENDPOINT" \
        --content-type "application/json" 2>&1 || {
        log_error "Upload with content-type failed"
        return 1
    }
    log_success "Upload with content-type completed"
    
    # Test 4: Verify Content-Type
    log "TEST 4: Verify Content-Type..."
    local HEAD_JSON=$(aws s3api head-object \
        --bucket $BUCKET \
        --key custom-type.json \
        --endpoint-url "$ENDPOINT" 2>&1)
    log "HeadObject response: $HEAD_JSON"
    
    if echo "$HEAD_JSON" | grep -q "application/json"; then
        log_success "Content-Type 'application/json' verified!"
    else
        log_error "Content-Type verification failed!"
        return 1
    fi
    
    # Test 5: Large metadata (close to 2KB limit)
    log "TEST 5: Upload with large metadata (1.5KB)..."
    # Generate a ~1.5KB metadata value
    local LARGE_VALUE=$(head -c 1500 /dev/urandom | base64 | tr -dc 'a-zA-Z0-9' | head -c 1400)
    aws s3 cp $TEST_FILE s3://$BUCKET/large-meta.txt \
        --endpoint-url "$ENDPOINT" \
        --metadata "largekey=$LARGE_VALUE" 2>&1 || {
        log_error "Upload with large metadata failed"
        return 1
    }
    log_success "Upload with large metadata completed"
    
    # Cleanup
    log "Cleanup..."
    aws s3 rm s3://$BUCKET --recursive --endpoint-url "$ENDPOINT" 2>&1
    aws s3 rb s3://$BUCKET --endpoint-url "$ENDPOINT" 2>&1
    rm -f $TEST_FILE
    
    log_success "All Metadata Tests Passed!"
    return 0
}

export -f run_metadata_tests
