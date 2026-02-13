#!/bin/bash
# Enhanced AWS CLI Blackbox Testing - Copy Operations
# Tests: CopyObject (same bucket, cross path)
set -e

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [COPY] $*"
}

log_success() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [COPY] ✅ $*"
}

log_error() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [COPY] ❌ $*" >&2
}

run_copy_tests() {
    local ENDPOINT=$1
    local BUCKET=$2
    
    log "Starting Copy Operations Tests"
    log "Endpoint: $ENDPOINT"
    log "Bucket: $BUCKET"
    
    local SRC_FILE="copy-source.txt"
    local DST_KEY="copy-destination.txt"
    local DOWNLOADED="copy-downloaded.txt"
    
    # Setup: Create bucket and upload source file
    log "Setup: Creating bucket and source file..."
    aws s3 mb s3://$BUCKET --endpoint-url "$ENDPOINT" 2>&1 || true
    
    echo "Copy test content $(date)" > $SRC_FILE
    local ORIGINAL_MD5=$(md5sum $SRC_FILE | cut -d' ' -f1)
    log "Source file MD5: $ORIGINAL_MD5"
    
    aws s3 cp $SRC_FILE s3://$BUCKET/ --endpoint-url "$ENDPOINT" 2>&1
    log_success "Source file uploaded"
    
    # Test 1: CopyObject within same bucket
    log "TEST 1: CopyObject (same bucket)..."
    aws s3 cp s3://$BUCKET/$SRC_FILE s3://$BUCKET/$DST_KEY --endpoint-url "$ENDPOINT" 2>&1 || {
        log_error "CopyObject failed"
        return 1
    }
    log_success "CopyObject command succeeded"
    
    # Verify copy by downloading
    log "Verifying copied object..."
    aws s3 cp s3://$BUCKET/$DST_KEY $DOWNLOADED --endpoint-url "$ENDPOINT" 2>&1
    local COPIED_MD5=$(md5sum $DOWNLOADED | cut -d' ' -f1)
    log "Copied file MD5: $COPIED_MD5"
    
    if [ "$ORIGINAL_MD5" = "$COPIED_MD5" ]; then
        log_success "CopyObject verification passed - MD5 match!"
    else
        log_error "CopyObject verification failed - MD5 mismatch!"
        return 1
    fi
    
    # Test 2: CopyObject to nested path
    log "TEST 2: CopyObject (to nested path)..."
    local NESTED_KEY="folder/subfolder/nested-copy.txt"
    aws s3 cp s3://$BUCKET/$SRC_FILE s3://$BUCKET/$NESTED_KEY --endpoint-url "$ENDPOINT" 2>&1 || {
        log_error "CopyObject to nested path failed"
        return 1
    }
    log_success "CopyObject to nested path succeeded"
    
    # Verify nested copy
    rm -f $DOWNLOADED
    aws s3 cp s3://$BUCKET/$NESTED_KEY $DOWNLOADED --endpoint-url "$ENDPOINT" 2>&1
    local NESTED_MD5=$(md5sum $DOWNLOADED | cut -d' ' -f1)
    
    if [ "$ORIGINAL_MD5" = "$NESTED_MD5" ]; then
        log_success "Nested CopyObject verification passed!"
    else
        log_error "Nested CopyObject verification failed!"
        return 1
    fi
    
    # Cleanup
    log "Cleanup..."
    aws s3 rm s3://$BUCKET --recursive --endpoint-url "$ENDPOINT" 2>&1
    aws s3 rb s3://$BUCKET --endpoint-url "$ENDPOINT" 2>&1
    rm -f $SRC_FILE $DOWNLOADED
    
    log_success "All Copy Operations Tests Passed!"
    return 0
}

export -f run_copy_tests
