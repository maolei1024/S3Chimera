package win.ixuni.chimera.core.util;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Upload lock manager
 * 
 * Prevents lock contention caused by concurrent database writes of multiple parts for the same uploadId.
 * 
 * Problem scenario:
 * - Rclone uploads multiple parts of the same file in parallel
 * - MySQL/PostgreSQL unique constraints cause gap locks
 * - Multiple concurrent INSERTs block each other, causing lock wait timeouts
 * 
 * Solution:
 * - Parts for the same uploadId are serialized for database writes
 * - Parts for different uploadIds can still be written in parallel
 */
@Slf4j
public class UploadLockManager {

    private static final UploadLockManager INSTANCE = new UploadLockManager();

    /**
     * One semaphore per uploadId, limiting concurrency to 1 (serial)
     */
    private final Map<String, LockEntry> locks = new ConcurrentHashMap<>();

    /**
     * Default lock wait timeout in seconds
     */
    private static final int DEFAULT_TIMEOUT_SECONDS = 60;

    /**
     * Lock entry containing a semaphore and reference count
     */
    private static class LockEntry {
        final Semaphore semaphore = new Semaphore(1);
        final AtomicInteger refCount = new AtomicInteger(0);
    }

    private UploadLockManager() {
    }

    public static UploadLockManager getInstance() {
        return INSTANCE;
    }

    /**
     * Execute an operation while holding the lock
     * 
     * @param uploadId  upload identifier
     * @param operation operation to execute
     * @return operation result
     */
    public <T> Mono<T> withLock(String uploadId, Mono<T> operation) {
        return withLock(uploadId, operation, DEFAULT_TIMEOUT_SECONDS);
    }

    /**
     * Execute an operation while holding the lock (with timeout)
     * 
     * @param uploadId       upload 标识符
     * @param operation      要执行的操作
     * @param timeoutSeconds lock wait timeout in seconds
     * @return operation result
     */
    public <T> Mono<T> withLock(String uploadId, Mono<T> operation, int timeoutSeconds) {
        return Mono.defer(() -> {
            LockEntry entry = locks.computeIfAbsent(uploadId, k -> new LockEntry());
            entry.refCount.incrementAndGet();

            return Mono.fromCallable(() -> {
                boolean acquired = entry.semaphore.tryAcquire(timeoutSeconds, TimeUnit.SECONDS);
                if (!acquired) {
                    throw new RuntimeException("Failed to acquire upload lock for " + uploadId +
                            " within " + timeoutSeconds + " seconds");
                }
                log.debug("[UPLOAD_LOCK] Acquired lock for uploadId={}", uploadId);
                return true;
            })
                    .subscribeOn(reactor.core.scheduler.Schedulers.boundedElastic())
                    .flatMap(acquired -> operation)
                    .doFinally(signal -> {
                        entry.semaphore.release();
                        log.debug("[UPLOAD_LOCK] Released lock for uploadId={}", uploadId);

                        // Clean up locks no longer in use
                        if (entry.refCount.decrementAndGet() == 0) {
                            locks.remove(uploadId, entry);
                        }
                    });
        });
    }

    /**
     * Clean up the lock for a given uploadId (called when upload completes or is aborted)
     */
    public void cleanupLock(String uploadId) {
        LockEntry removed = locks.remove(uploadId);
        if (removed != null) {
            log.debug("[UPLOAD_LOCK] Cleaned up lock for uploadId={}", uploadId);
        }
    }

    /**
     * Get the count of currently active locks (for monitoring)
     */
    public int getActiveLockCount() {
        return locks.size();
    }
}
