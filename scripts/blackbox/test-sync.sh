#!/bin/bash
# Enhanced AWS CLI Blackbox Testing - Sync/Batch Operations
# Tests: aws s3 sync for batch upload/download of 100+ files
set -e

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [SYNC] $*"
}

log_success() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [SYNC] ✅ $*"
}

log_error() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [SYNC] ❌ $*" >&2
}

run_sync_tests() {
    local ENDPOINT=$1
    local BUCKET=$2
    local FILE_COUNT=${3:-100}  # Default 100 files
    
    log "Starting Sync/Batch Operations Tests"
    log "Endpoint: $ENDPOINT"
    log "Bucket: $BUCKET"
    log "File count: $FILE_COUNT"
    
    local UPLOAD_DIR="sync-upload-test"
    local DOWNLOAD_DIR="sync-download-test"
    
    # Setup: Create bucket
    log "Setup: Creating bucket..."
    aws s3 mb s3://$BUCKET --endpoint-url "$ENDPOINT" 2>&1 || true
    
    # Generate test files
    log "Generating $FILE_COUNT test files..."
    rm -rf $UPLOAD_DIR $DOWNLOAD_DIR
    mkdir -p $UPLOAD_DIR
    
    for i in $(seq 1 $FILE_COUNT); do
        echo "File $i content: $(date) - $RANDOM" > "$UPLOAD_DIR/file-$(printf '%03d' $i).txt"
    done
    log "Generated $FILE_COUNT files in $UPLOAD_DIR"
    
    # Calculate total MD5 of all files (sorted)
    local UPLOAD_MD5=$(cat $UPLOAD_DIR/*.txt | md5sum | cut -d' ' -f1)
    log "Combined upload MD5: $UPLOAD_MD5"
    
    # Test 1: Sync upload
    log "TEST 1: Sync upload ($FILE_COUNT files)..."
    local UPLOAD_START=$(date +%s)
    aws s3 sync $UPLOAD_DIR s3://$BUCKET/sync-test/ --endpoint-url "$ENDPOINT" 2>&1 || {
        log_error "Sync upload failed"
        return 1
    }
    local UPLOAD_TIME=$(($(date +%s) - UPLOAD_START))
    log_success "Sync upload completed in ${UPLOAD_TIME}s"
    
    # Test 2: Verify all files uploaded
    log "TEST 2: Verify uploaded file count..."
    local UPLOADED_COUNT=$(aws s3 ls s3://$BUCKET/sync-test/ --endpoint-url "$ENDPOINT" 2>&1 | wc -l)
    log "Uploaded file count: $UPLOADED_COUNT"
    
    if [ "$UPLOADED_COUNT" -eq "$FILE_COUNT" ]; then
        log_success "File count verification passed! All $FILE_COUNT files uploaded."
    else
        log_error "File count mismatch! Expected: $FILE_COUNT, Got: $UPLOADED_COUNT"
        return 1
    fi
    
    # Test 3: Sync download
    log "TEST 3: Sync download ($FILE_COUNT files)..."
    mkdir -p $DOWNLOAD_DIR
    local DOWNLOAD_START=$(date +%s)
    aws s3 sync s3://$BUCKET/sync-test/ $DOWNLOAD_DIR --endpoint-url "$ENDPOINT" 2>&1 || {
        log_error "Sync download failed"
        return 1
    }
    local DOWNLOAD_TIME=$(($(date +%s) - DOWNLOAD_START))
    log_success "Sync download completed in ${DOWNLOAD_TIME}s"
    
    # Test 4: Verify content integrity
    log "TEST 4: Verify downloaded content integrity..."
    local DOWNLOAD_MD5=$(cat $DOWNLOAD_DIR/*.txt | md5sum | cut -d' ' -f1)
    log "Combined download MD5: $DOWNLOAD_MD5"
    
    if [ "$UPLOAD_MD5" = "$DOWNLOAD_MD5" ]; then
        log_success "Content integrity verification passed!"
    else
        log_error "Content integrity failed! MD5 mismatch."
        return 1
    fi
    
    # Cleanup
    log "Cleanup..."
    aws s3 rm s3://$BUCKET --recursive --endpoint-url "$ENDPOINT" 2>&1
    aws s3 rb s3://$BUCKET --endpoint-url "$ENDPOINT" 2>&1
    rm -rf $UPLOAD_DIR $DOWNLOAD_DIR
    
    log_success "All Sync/Batch Operations Tests Passed!"
    log "Summary: Uploaded $FILE_COUNT files in ${UPLOAD_TIME}s, Downloaded in ${DOWNLOAD_TIME}s"
    return 0
}

export -f run_sync_tests
