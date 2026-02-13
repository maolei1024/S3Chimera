#!/bin/bash
# Enhanced AWS CLI Blackbox Testing - Large File / Multipart Upload
# Tests: Large file upload (512MB), download, and MD5 verification
set -e

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [MULTIPART] $*"
}

log_success() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [MULTIPART] ✅ $*"
}

log_error() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [MULTIPART] ❌ $*" >&2
}

run_multipart_tests() {
    local ENDPOINT=$1
    local BUCKET=$2
    local LARGE_SIZE_MB=${3:-512}  # Default 512MB
    
    log "Starting Multipart/Large File Tests"
    log "Endpoint: $ENDPOINT"
    log "Bucket: $BUCKET"
    log "Large file size: ${LARGE_SIZE_MB}MB"
    
    local LARGE_FILE="large-file-${LARGE_SIZE_MB}MB.bin"
    local DOWNLOADED="large-downloaded.bin"
    
    # Setup: Create bucket
    log "Setup: Creating bucket..."
    aws s3 mb s3://$BUCKET --endpoint-url "$ENDPOINT" 2>&1 || true
    
    # Generate large file
    log "Generating ${LARGE_SIZE_MB}MB test file (this may take a while)..."
    local START_TIME=$(date +%s)
    dd if=/dev/urandom of=$LARGE_FILE bs=1M count=$LARGE_SIZE_MB status=progress 2>&1
    local GEN_TIME=$(($(date +%s) - START_TIME))
    log "File generation completed in ${GEN_TIME}s"
    
    local ORIGINAL_SIZE=$(stat -c%s $LARGE_FILE)
    local ORIGINAL_MD5=$(md5sum $LARGE_FILE | cut -d' ' -f1)
    log "Original file size: $ORIGINAL_SIZE bytes"
    log "Original file MD5: $ORIGINAL_MD5"
    
    # Test 1: Upload large file (triggers multipart)
    log "TEST 1: Upload ${LARGE_SIZE_MB}MB file..."
    local UPLOAD_START=$(date +%s)
    aws s3 cp $LARGE_FILE s3://$BUCKET/ --endpoint-url "$ENDPOINT" 2>&1 || {
        log_error "Large file upload failed"
        rm -f $LARGE_FILE
        return 1
    }
    local UPLOAD_TIME=$(($(date +%s) - UPLOAD_START))
    local UPLOAD_SPEED=$((LARGE_SIZE_MB / (UPLOAD_TIME + 1)))
    log_success "Upload completed in ${UPLOAD_TIME}s (~${UPLOAD_SPEED}MB/s)"
    
    # Test 2: Verify object exists and size matches
    log "TEST 2: Verify object metadata..."
    local HEAD_RESULT=$(aws s3api head-object --bucket $BUCKET --key $LARGE_FILE --endpoint-url "$ENDPOINT" 2>&1)
    local REPORTED_SIZE=$(echo "$HEAD_RESULT" | grep -o '"ContentLength": [0-9]*' | grep -o '[0-9]*')
    log "Reported ContentLength: $REPORTED_SIZE"
    
    if [ "$ORIGINAL_SIZE" = "$REPORTED_SIZE" ]; then
        log_success "Size verification passed!"
    else
        log_error "Size mismatch! Original: $ORIGINAL_SIZE, Reported: $REPORTED_SIZE"
        return 1
    fi
    
    # Test 3: Download and verify MD5
    log "TEST 3: Download ${LARGE_SIZE_MB}MB file..."
    local DOWNLOAD_START=$(date +%s)
    aws s3 cp s3://$BUCKET/$LARGE_FILE $DOWNLOADED --endpoint-url "$ENDPOINT" 2>&1 || {
        log_error "Large file download failed"
        rm -f $LARGE_FILE
        return 1
    }
    local DOWNLOAD_TIME=$(($(date +%s) - DOWNLOAD_START))
    local DOWNLOAD_SPEED=$((LARGE_SIZE_MB / (DOWNLOAD_TIME + 1)))
    log_success "Download completed in ${DOWNLOAD_TIME}s (~${DOWNLOAD_SPEED}MB/s)"
    
    log "Calculating downloaded file MD5..."
    local DOWNLOADED_MD5=$(md5sum $DOWNLOADED | cut -d' ' -f1)
    log "Downloaded file MD5: $DOWNLOADED_MD5"
    
    if [ "$ORIGINAL_MD5" = "$DOWNLOADED_MD5" ]; then
        log_success "MD5 verification passed! Large file integrity confirmed."
    else
        log_error "MD5 mismatch! Data corruption detected!"
        log_error "Expected: $ORIGINAL_MD5"
        log_error "Got: $DOWNLOADED_MD5"
        return 1
    fi
    
    # Cleanup
    log "Cleanup..."
    aws s3 rm s3://$BUCKET --recursive --endpoint-url "$ENDPOINT" 2>&1
    aws s3 rb s3://$BUCKET --endpoint-url "$ENDPOINT" 2>&1
    rm -f $LARGE_FILE $DOWNLOADED
    
    log_success "All Multipart/Large File Tests Passed!"
    log "Summary: Uploaded ${LARGE_SIZE_MB}MB in ${UPLOAD_TIME}s, Downloaded in ${DOWNLOAD_TIME}s"
    return 0
}

export -f run_multipart_tests
