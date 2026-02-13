package win.ixuni.chimera.driver.memory;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.config.DriverConfig;
import win.ixuni.chimera.core.driver.AbstractStorageDriverV2;
import win.ixuni.chimera.core.driver.DriverCapabilities;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.driver.memory.context.MemoryDriverContext;
import win.ixuni.chimera.driver.memory.handler.bucket.MemoryBucketExistsHandler;
import win.ixuni.chimera.driver.memory.handler.bucket.MemoryCreateBucketHandler;
import win.ixuni.chimera.driver.memory.handler.bucket.MemoryDeleteBucketHandler;
import win.ixuni.chimera.driver.memory.handler.bucket.MemoryListBucketsHandler;
import win.ixuni.chimera.driver.memory.handler.multipart.*;
import win.ixuni.chimera.driver.memory.handler.object.*;

import java.util.*;

/**
 * Memory 存储驱动 V2
 * <p>
 * Command-pattern-based in-memory driver, using handlers for all operations.
 * All S3 operations are executed through registered handlers, maintaining architectural consistency.
 */
@Slf4j
public class MemoryStorageDriverV2 extends AbstractStorageDriverV2 {

    @Getter
    private final DriverConfig config;

    private final MemoryDriverContext driverContext;

    public MemoryStorageDriverV2(DriverConfig config) {
        this.config = config;
        this.driverContext = MemoryDriverContext.builder()
                .config(config)
                .build();
        registerHandlers();
        // Inject handlerRegistry into context so handlers can invoke other operations via context.execute()
        driverContext.setHandlerRegistry(getHandlerRegistry());
    }

    private void registerHandlers() {
        // Bucket handlers (4)
        getHandlerRegistry().register(new MemoryCreateBucketHandler());
        getHandlerRegistry().register(new MemoryDeleteBucketHandler());
        getHandlerRegistry().register(new MemoryBucketExistsHandler());
        getHandlerRegistry().register(new MemoryListBucketsHandler());

        // Object handlers (9)
        getHandlerRegistry().register(new MemoryPutObjectHandler());
        getHandlerRegistry().register(new MemoryGetObjectHandler());
        getHandlerRegistry().register(new MemoryHeadObjectHandler());
        getHandlerRegistry().register(new MemoryDeleteObjectHandler());
        getHandlerRegistry().register(new MemoryListObjectsHandler());
        getHandlerRegistry().register(new MemoryListObjectsV2Handler());
        getHandlerRegistry().register(new MemoryCopyObjectHandler());
        getHandlerRegistry().register(new MemoryDeleteObjectsHandler());
        getHandlerRegistry().register(new MemoryGetObjectRangeHandler());

        // Multipart handlers (6)
        getHandlerRegistry().register(new MemoryCreateMultipartUploadHandler());
        getHandlerRegistry().register(new MemoryUploadPartHandler());
        getHandlerRegistry().register(new MemoryCompleteMultipartUploadHandler());
        getHandlerRegistry().register(new MemoryAbortMultipartUploadHandler());
        getHandlerRegistry().register(new MemoryListPartsHandler());
        getHandlerRegistry().register(new MemoryListMultipartUploadsHandler());

        log.info("Registered {} operation handlers for memory driver", getHandlerRegistry().size());
    }

    @Override
    public DriverContext getDriverContext() {
        return driverContext;
    }

    @Override
    public String getDriverType() {
        return "memory";
    }

    @Override
    public String getDriverName() {
        return config.getName();
    }

    // getCapabilities() is now dynamically inferred by parent class AbstractStorageDriverV2

    @Override
    public Mono<Void> initialize() {
        log.info("Initializing memory storage driver V2: {}", config.getName());
        return Mono.empty();
    }

    @Override
    public Mono<Void> shutdown() {
        log.info("Shutting down memory storage driver V2: {}", config.getName());
        driverContext.getBuckets().clear();
        driverContext.getObjects().clear();
        driverContext.getMultipartUploads().clear();
        return Mono.empty();
    }
}
