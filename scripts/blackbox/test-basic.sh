#!/bin/bash
# Enhanced AWS CLI Blackbox Testing - Basic Operations
# Tests: CreateBucket, ListBuckets, PutObject, GetObject, HeadObject, ListObjects, DeleteObject, DeleteBucket
set -e

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [BASIC] $*"
}

log_success() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [BASIC] ✅ $*"
}

log_error() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [BASIC] ❌ $*" >&2
}

run_basic_tests() {
    local ENDPOINT=$1
    local BUCKET=$2
    
    log "Starting Basic Operations Tests"
    log "Endpoint: $ENDPOINT"
    log "Bucket: $BUCKET"
    
    local TEST_FILE="basic-test-file.txt"
    local DOWNLOADED="basic-downloaded.txt"
    
    # Create test file
    log "Creating test file..."
    echo "Hello S3Chimera Basic Test $(date)" > $TEST_FILE
    local ORIGINAL_MD5=$(md5sum $TEST_FILE | cut -d' ' -f1)
    log "Original file MD5: $ORIGINAL_MD5"
    
    # Test 1: Create Bucket
    log "TEST 1: CreateBucket..."
    aws s3 mb s3://$BUCKET --endpoint-url "$ENDPOINT" 2>&1 || {
        log_error "CreateBucket failed"
        return 1
    }
    log_success "CreateBucket passed"
    
    # Test 2: List Buckets
    log "TEST 2: ListBuckets..."
    local BUCKET_LIST=$(aws s3 ls --endpoint-url "$ENDPOINT" 2>&1)
    if echo "$BUCKET_LIST" | grep -q "$BUCKET"; then
        log_success "ListBuckets passed - found bucket: $BUCKET"
    else
        log_error "ListBuckets failed - bucket not found"
        echo "Bucket list: $BUCKET_LIST"
        return 1
    fi
    
    # Test 3: PutObject
    log "TEST 3: PutObject..."
    aws s3 cp $TEST_FILE s3://$BUCKET/ --endpoint-url "$ENDPOINT" 2>&1 || {
        log_error "PutObject failed"
        return 1
    }
    log_success "PutObject passed"
    
    # Test 4: ListObjects
    log "TEST 4: ListObjects..."
    local OBJECT_LIST=$(aws s3 ls s3://$BUCKET/ --endpoint-url "$ENDPOINT" 2>&1)
    if echo "$OBJECT_LIST" | grep -q "$TEST_FILE"; then
        log_success "ListObjects passed - found object: $TEST_FILE"
    else
        log_error "ListObjects failed - object not found"
        echo "Object list: $OBJECT_LIST"
        return 1
    fi
    
    # Test 5: HeadObject
    log "TEST 5: HeadObject..."
    local HEAD_RESULT=$(aws s3api head-object --bucket $BUCKET --key $TEST_FILE --endpoint-url "$ENDPOINT" 2>&1)
    if echo "$HEAD_RESULT" | grep -q "ContentLength"; then
        log_success "HeadObject passed"
        log "HeadObject response: $HEAD_RESULT"
    else
        log_error "HeadObject failed"
        echo "HeadObject response: $HEAD_RESULT"
        return 1
    fi
    
    # Test 6: GetObject
    log "TEST 6: GetObject..."
    aws s3 cp s3://$BUCKET/$TEST_FILE $DOWNLOADED --endpoint-url "$ENDPOINT" 2>&1 || {
        log_error "GetObject failed"
        return 1
    }
    local DOWNLOADED_MD5=$(md5sum $DOWNLOADED | cut -d' ' -f1)
    log "Downloaded file MD5: $DOWNLOADED_MD5"
    
    if [ "$ORIGINAL_MD5" = "$DOWNLOADED_MD5" ]; then
        log_success "GetObject passed - MD5 match!"
    else
        log_error "GetObject failed - MD5 mismatch!"
        log_error "Expected: $ORIGINAL_MD5, Got: $DOWNLOADED_MD5"
        return 1
    fi
    
    # Test 7: DeleteObject
    log "TEST 7: DeleteObject..."
    aws s3 rm s3://$BUCKET/$TEST_FILE --endpoint-url "$ENDPOINT" 2>&1 || {
        log_error "DeleteObject failed"
        return 1
    }
    log_success "DeleteObject passed"
    
    # Test 8: DeleteBucket
    log "TEST 8: DeleteBucket..."
    aws s3 rb s3://$BUCKET --endpoint-url "$ENDPOINT" 2>&1 || {
        log_error "DeleteBucket failed"
        return 1
    }
    log_success "DeleteBucket passed"
    
    # Cleanup
    rm -f $TEST_FILE $DOWNLOADED
    
    log_success "All Basic Operations Tests Passed!"
    return 0
}

# Export for use by main script
export -f run_basic_tests log log_success log_error
