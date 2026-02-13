package win.ixuni.chimera.core.driver;

import reactor.core.publisher.Mono;

/**
 * Interface for cleanable storage drivers
 * 
 * Drivers that support cleaning up orphan data and expired sessions should implement this interface
 */
public interface CleanableDriver {

    /**
     * Clean up orphan chunks (chunks whose corresponding multipart upload no longer exists or has timed out)
     * 
     * @param olderThanHours clean up data older than this many hours
     * @return number of chunks cleaned up
     */
    Mono<Long> cleanupOrphanChunks(int olderThanHours);

    /**
     * Clean up expired multipart upload sessions (status=0 but timed out)
     * 
     * @param olderThanHours clean up sessions older than this many hours
     * @return number of sessions cleaned up
     */
    Mono<Long> cleanupExpiredUploads(int olderThanHours);

    /**
     * Perform a full cleanup task
     * 
     * @param olderThanHours clean up data older than this many hours
     * @return 清理结果
     */
    Mono<CleanupResult> performCleanup(int olderThanHours);

    /**
     * Cleanup result
     */
    record CleanupResult(long expiredUploads, long orphanChunks) {
        @Override
        public String toString() {
            return "CleanupResult{expiredUploads=" + expiredUploads + ", orphanChunks=" + orphanChunks + "}";
        }
    }
}
