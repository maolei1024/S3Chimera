package win.ixuni.chimera.server.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import win.ixuni.chimera.core.driver.CleanableDriver;
import win.ixuni.chimera.server.registry.DriverRegistry;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Scheduled cleanup service
 * 
 * 负责清理:
 * - 过期的分片上传会话
 * - 孤儿 chunks (上传失败或中断留下的数据)
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class CleanupSchedulerService {

    private final DriverRegistry driverRegistry;
    
    @Value("${chimera.cleanup.enabled:true}")
    private boolean cleanupEnabled;
    
    @Value("${chimera.cleanup.older-than-hours:24}")
    private int olderThanHours;
    
    private final AtomicBoolean running = new AtomicBoolean(false);

    /**
     * Execute cleanup task every hour
     * Can be disabled via chimera.cleanup.enabled=false
     */
    @Scheduled(cron = "${chimera.cleanup.cron:0 0 * * * *}")
    public void scheduledCleanup() {
        if (!cleanupEnabled) {
            log.debug("Cleanup is disabled, skipping scheduled cleanup");
            return;
        }
        
        if (!running.compareAndSet(false, true)) {
            log.warn("Previous cleanup is still running, skipping this run");
            return;
        }
        
        try {
            log.info("Starting scheduled cleanup...");
            performCleanup();
        } finally {
            running.set(false);
        }
    }

    /**
     * 手动触发清理
     */
    public void triggerCleanup() {
        if (!running.compareAndSet(false, true)) {
            log.warn("Cleanup is already running");
            return;
        }
        
        try {
            log.info("Manual cleanup triggered...");
            performCleanup();
        } finally {
            running.set(false);
        }
    }

    private void performCleanup() {
        // Iterate all drivers, execute cleanup on those that support it
        driverRegistry.getAllDrivers().values().forEach(driver -> {
            if (driver instanceof CleanableDriver cleanableDriver) {
                log.info("Performing cleanup for driver: {}", driver.getDriverName());
                try {
                    var result = cleanableDriver.performCleanup(olderThanHours).block();
                    if (result != null) {
                        log.info("Cleanup result for {}: expiredUploads={}, orphanChunks={}",
                                driver.getDriverName(), result.expiredUploads(), result.orphanChunks());
                    }
                } catch (Exception e) {
                    log.error("Cleanup failed for driver {}: {}", driver.getDriverName(), e.getMessage(), e);
                }
            }
        });
    }
}
