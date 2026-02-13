#!/bin/bash
# ============================================================================
# S3Chimera Enhanced AWS CLI Blackbox Testing Framework
# ============================================================================
# This script orchestrates all blackbox tests for S3 compatibility verification.
# 
# Test Modules:
#   - test-basic.sh      : Basic CRUD operations
#   - test-copy.sh       : CopyObject operations  
#   - test-multipart.sh  : Large file (512MB) multipart upload
#   - test-sync.sh       : Batch sync operations (100+ files)
#   - test-metadata.sh   : User metadata and Content-Type
#   - test-edge-cases.sh : Special characters, deep paths, batch delete
#
# Usage:
#   ./verify-compatibility.sh [endpoint] [access-key] [secret-key] [region]
#
# Environment Variables:
#   SKIP_MULTIPART=true  : Skip 512MB large file test (for faster CI)
#   FILE_COUNT=50        : Number of files for sync test (default: 100)
#   LARGE_SIZE_MB=128    : Large file size in MB (default: 512)
# ============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Configuration
ENDPOINT=${1:-"http://127.0.0.1:8080"}
ACCESS_KEY=${2:-"test-access-key"}
SECRET_KEY=${3:-"test-secret-key"}
REGION=${4:-"us-east-1"}

# Test parameters (can be overridden via environment)
LARGE_SIZE_MB=${LARGE_SIZE_MB:-512}
FILE_COUNT=${FILE_COUNT:-100}
SKIP_MULTIPART=${SKIP_MULTIPART:-false}

# Export AWS credentials
export AWS_ACCESS_KEY_ID=$ACCESS_KEY
export AWS_SECRET_ACCESS_KEY=$SECRET_KEY
export AWS_DEFAULT_REGION=$REGION

# Logging functions
log_header() {
    echo ""
    echo "============================================================================"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
    echo "============================================================================"
}

log_info() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [INFO] $*"
}

log_success() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [SUCCESS] ✅ $*"
}

log_error() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [ERROR] ❌ $*" >&2
}

log_skip() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [SKIP] ⏭️ $*"
}

# Source test modules
source "$SCRIPT_DIR/blackbox/test-basic.sh"
source "$SCRIPT_DIR/blackbox/test-copy.sh"
source "$SCRIPT_DIR/blackbox/test-multipart.sh"
source "$SCRIPT_DIR/blackbox/test-sync.sh"
source "$SCRIPT_DIR/blackbox/test-metadata.sh"
source "$SCRIPT_DIR/blackbox/test-edge-cases.sh"

# Track test results
PASSED=0
FAILED=0
SKIPPED=0
declare -a FAILED_TESTS

run_test_module() {
    local TEST_NAME=$1
    local TEST_FUNC=$2
    local BUCKET="blackbox-${TEST_NAME}-$(date +%s)"
    
    log_header "Running Test Module: $TEST_NAME"
    
    if $TEST_FUNC "$ENDPOINT" "$BUCKET" "${@:3}"; then
        log_success "Test Module '$TEST_NAME' PASSED"
        ((PASSED++))
    else
        log_error "Test Module '$TEST_NAME' FAILED"
        ((FAILED++))
        FAILED_TESTS+=("$TEST_NAME")
    fi
}

# ============================================================================
# Main Test Execution
# ============================================================================

log_header "S3Chimera Enhanced Blackbox Testing"
log_info "Endpoint: $ENDPOINT"
log_info "Driver: ${CHIMERA_DRIVER_TYPE:-unknown}"
log_info "Large file size: ${LARGE_SIZE_MB}MB"
log_info "Sync file count: $FILE_COUNT"
log_info "Skip multipart: $SKIP_MULTIPART"

START_TIME=$(date +%s)

# Run test modules
run_test_module "basic" run_basic_tests

run_test_module "copy" run_copy_tests

run_test_module "metadata" run_metadata_tests

run_test_module "edge-cases" run_edge_case_tests

run_test_module "sync" run_sync_tests "$FILE_COUNT"

# Multipart test (optional skip for faster CI)
if [ "$SKIP_MULTIPART" = "true" ]; then
    log_skip "Multipart/Large File Test (SKIP_MULTIPART=true)"
    ((SKIPPED++))
else
    run_test_module "multipart" run_multipart_tests "$LARGE_SIZE_MB"
fi

# ============================================================================
# Summary Report
# ============================================================================

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

log_header "Test Summary"
echo ""
echo "┌─────────────────────────────────────────────────────────────┐"
echo "│                   BLACKBOX TEST RESULTS                     │"
echo "├─────────────────────────────────────────────────────────────┤"
printf "│  %-20s : %-36s │\n" "Endpoint" "$ENDPOINT"
printf "│  %-20s : %-36s │\n" "Driver" "${CHIMERA_DRIVER_TYPE:-unknown}"
printf "│  %-20s : %-36s │\n" "Duration" "${DURATION}s"
echo "├─────────────────────────────────────────────────────────────┤"
printf "│  %-20s : %-36s │\n" "PASSED" "$PASSED"
printf "│  %-20s : %-36s │\n" "FAILED" "$FAILED"
printf "│  %-20s : %-36s │\n" "SKIPPED" "$SKIPPED"
echo "└─────────────────────────────────────────────────────────────┘"
echo ""

if [ ${#FAILED_TESTS[@]} -gt 0 ]; then
    log_error "Failed test modules:"
    for test in "${FAILED_TESTS[@]}"; do
        echo "  - $test"
    done
    echo ""
    exit 1
else
    log_success "All blackbox tests passed!"
    exit 0
fi
