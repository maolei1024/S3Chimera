package win.ixuni.chimera.core.driver;

import win.ixuni.chimera.core.config.DriverConfig;

/**
 * Driver factory interface
 * <p>
 * Each driver type provides a factory implementation to create driver instances from configuration.
 * Supports creating multiple instances of the same driver type (e.g. multiple MySQL databases).
 */
public interface DriverFactory {

    /**
     * Get the driver type supported by this factory
     *
     * @return driver type identifier (e.g. "memory", "sql", "webdav")
     */
    String getDriverType();

    /**
     * Create a driver instance from configuration
     *
     * @param config driver configuration
     * @return driver instance
     */
    StorageDriver createDriver(DriverConfig config);

    /**
     * Get the driver description
     *
     * @return description text
     */
    default String getDescription() {
        return getDriverType() + " storage driver";
    }
}
