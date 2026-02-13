package win.ixuni.chimera.core.driver;

import reactor.core.publisher.Mono;

import java.util.EnumSet;
import java.util.Set;

/**
 * Base storage driver interface
 * <p>
 * Defines driver metadata and lifecycle methods.
 * All S3 operations are executed via the {@link StorageDriverV2#execute} method.
 */
public interface StorageDriver extends DriverCapabilities {

    // ==================== Driver Metadata ====================

    /**
     * Get the driver type identifier
     *
     * @return driver type (e.g. "memory", "mysql", "webdav")
     */
    String getDriverType();

    /**
     * Get the driver instance name
     *
     * @return instance name (as specified in configuration)
     */
    String getDriverName();

    /**
     * Get the capabilities supported by this driver
     *
     * @return the set of supported capabilities
     */
    @Override
    default Set<Capability> getCapabilities() {
        return EnumSet.of(
                Capability.READ,
                Capability.WRITE,
                Capability.MULTIPART_UPLOAD,
                Capability.COPY,
                Capability.BATCH_DELETE);
    }

    // ==================== Lifecycle ====================

    /**
     * Initialize the driver
     *
     * @return completion signal
     */
    default Mono<Void> initialize() {
        return Mono.empty();
    }

    /**
     * Shut down the driver and release resources
     *
     * @return completion signal
     */
    default Mono<Void> shutdown() {
        return Mono.empty();
    }
}
