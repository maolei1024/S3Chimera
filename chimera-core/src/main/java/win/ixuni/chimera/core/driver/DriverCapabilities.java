package win.ixuni.chimera.core.driver;

import java.util.Set;

/**
 * Driver capability descriptor
 * <p>
 * Defines the set of features a driver supports, allowing drivers to declare unsupported operations.
 * The controller layer can decide whether to accept requests based on driver capabilities.
 */
public interface DriverCapabilities {

    /**
     * Driver capability enumeration
     */
    enum Capability {
        /**
         * Basic read operations (GET Object, HEAD Object, List Objects)
         */
        READ,

        /**
         * Basic write operations (PUT Object, DELETE Object)
         */
        WRITE,

        /**
         * Multipart Upload
         */
        MULTIPART_UPLOAD,

        /**
         * Range requests (resumable downloads, partial reads)
         */
        RANGE_READ,

        /**
         * Object copy (COPY Object)
         */
        COPY,

        /**
         * Batch delete (Delete Objects)
         */
        BATCH_DELETE,

        /**
         * Versioning
         */
        VERSIONING,

        /**
         * Server-side encryption
         */
        SERVER_SIDE_ENCRYPTION,

        /**
         * Object Lock
         */
        OBJECT_LOCK,

        /**
         * Lifecycle management
         */
        LIFECYCLE,

        /**
         * Storage Class
         */
        STORAGE_CLASS
    }

    /**
     * Get the set of capabilities supported by this driver
     *
     * @return the set of supported capabilities
     */
    Set<Capability> getCapabilities();

    /**
     * Check whether a specific capability is supported
     *
     * @param capability the capability to check
     * @return true if supported
     */
    default boolean supports(Capability capability) {
        return getCapabilities().contains(capability);
    }

    /**
     * Check whether all specified capabilities are supported
     *
     * @param capabilities the capabilities to check
     * @return true if all are supported
     */
    default boolean supportsAll(Capability... capabilities) {
        Set<Capability> caps = getCapabilities();
        for (Capability cap : capabilities) {
            if (!caps.contains(cap)) {
                return false;
            }
        }
        return true;
    }
}
