#!/bin/bash
# Enhanced AWS CLI Blackbox Testing - Edge Cases
# Tests: Special characters, deep paths, empty files, ListObjectsV2, DeleteObjects
set -e

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [EDGE] $*"
}

log_success() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [EDGE] ✅ $*"
}

log_error() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [EDGE] ❌ $*" >&2
}

run_edge_case_tests() {
    local ENDPOINT=$1
    local BUCKET=$2
    
    log "Starting Edge Case Tests"
    log "Endpoint: $ENDPOINT"
    log "Bucket: $BUCKET"
    
    local TEST_FILE="edge-test.txt"
    
    # Setup
    log "Setup: Creating bucket and test file..."
    aws s3 mb s3://$BUCKET --endpoint-url "$ENDPOINT" 2>&1 || true
    echo "Edge case test content" > $TEST_FILE
    
    # Test 1: Empty file
    log "TEST 1: Upload empty file..."
    touch empty-file.txt
    aws s3 cp empty-file.txt s3://$BUCKET/ --endpoint-url "$ENDPOINT" 2>&1 || {
        log_error "Empty file upload failed"
        return 1
    }
    
    local EMPTY_HEAD=$(aws s3api head-object --bucket $BUCKET --key empty-file.txt --endpoint-url "$ENDPOINT" 2>&1)
    local EMPTY_SIZE=$(echo "$EMPTY_HEAD" | grep -o '"ContentLength": [0-9]*' | grep -o '[0-9]*')
    if [ "$EMPTY_SIZE" = "0" ]; then
        log_success "Empty file upload verified (size=0)"
    else
        log_error "Empty file size mismatch: $EMPTY_SIZE"
        return 1
    fi
    
    # Test 2: Deep nested path
    log "TEST 2: Upload to deep nested path..."
    local DEEP_KEY="level1/level2/level3/level4/level5/deep-file.txt"
    aws s3 cp $TEST_FILE "s3://$BUCKET/$DEEP_KEY" --endpoint-url "$ENDPOINT" 2>&1 || {
        log_error "Deep path upload failed"
        return 1
    }
    
    aws s3api head-object --bucket $BUCKET --key "$DEEP_KEY" --endpoint-url "$ENDPOINT" 2>&1 && \
        log_success "Deep nested path verified" || {
        log_error "Deep nested path verification failed"
        return 1
    }
    
    # Test 3: File with spaces in name
    log "TEST 3: Upload file with spaces in name..."
    local SPACE_KEY="file with spaces.txt"
    aws s3 cp $TEST_FILE "s3://$BUCKET/$SPACE_KEY" --endpoint-url "$ENDPOINT" 2>&1 || {
        log_error "Upload with spaces failed"
        return 1
    }
    
    aws s3api head-object --bucket $BUCKET --key "$SPACE_KEY" --endpoint-url "$ENDPOINT" 2>&1 && \
        log_success "File with spaces verified" || {
        log_error "File with spaces verification failed"
        return 1
    }
    
    # Test 4: ListObjectsV2 with prefix
    log "TEST 4: ListObjectsV2 with prefix..."
    local LIST_V2=$(aws s3api list-objects-v2 \
        --bucket $BUCKET \
        --prefix "level1/" \
        --endpoint-url "$ENDPOINT" 2>&1)
    log "ListObjectsV2 response: $LIST_V2"
    
    if echo "$LIST_V2" | grep -q "deep-file.txt"; then
        log_success "ListObjectsV2 with prefix verified"
    else
        log_error "ListObjectsV2 with prefix failed"
        return 1
    fi
    
    # Test 5: Batch delete (DeleteObjects)
    log "TEST 5: Batch delete (DeleteObjects)..."
    # Create multiple files for batch delete
    for i in 1 2 3 4 5; do
        aws s3 cp $TEST_FILE "s3://$BUCKET/batch-delete-$i.txt" --endpoint-url "$ENDPOINT" 2>&1
    done
    
    # Use delete-objects API
    aws s3api delete-objects \
        --bucket $BUCKET \
        --delete '{
            "Objects": [
                {"Key": "batch-delete-1.txt"},
                {"Key": "batch-delete-2.txt"},
                {"Key": "batch-delete-3.txt"},
                {"Key": "batch-delete-4.txt"},
                {"Key": "batch-delete-5.txt"}
            ]
        }' \
        --endpoint-url "$ENDPOINT" 2>&1 || {
        log_error "Batch delete failed"
        return 1
    }
    log_success "Batch delete completed"
    
    # Verify deletion
    local REMAINING=$(aws s3 ls s3://$BUCKET/ --endpoint-url "$ENDPOINT" 2>&1 | grep "batch-delete" | wc -l)
    if [ "$REMAINING" = "0" ]; then
        log_success "Batch delete verified - all files removed"
    else
        log_error "Batch delete verification failed - $REMAINING files remain"
        return 1
    fi
    
    # Cleanup
    log "Cleanup..."
    aws s3 rm s3://$BUCKET --recursive --endpoint-url "$ENDPOINT" 2>&1
    aws s3 rb s3://$BUCKET --endpoint-url "$ENDPOINT" 2>&1
    rm -f $TEST_FILE empty-file.txt
    
    log_success "All Edge Case Tests Passed!"
    return 0
}

export -f run_edge_case_tests
