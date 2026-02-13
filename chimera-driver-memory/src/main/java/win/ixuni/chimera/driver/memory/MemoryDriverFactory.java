package win.ixuni.chimera.driver.memory;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import win.ixuni.chimera.core.config.DriverConfig;
import win.ixuni.chimera.core.driver.DriverFactory;
import win.ixuni.chimera.core.driver.StorageDriver;

/**
 * In-memory driver factory
 */
@Slf4j
@Component
public class MemoryDriverFactory implements DriverFactory {

    public static final String DRIVER_TYPE = "memory";

    @Override
    public String getDriverType() {
        return DRIVER_TYPE;
    }

    @Override
    public StorageDriver createDriver(DriverConfig config) {
        log.info("Creating memory driver V2 instance: {}", config.getName());
        return new MemoryStorageDriverV2(config);
    }

    @Override
    public String getDescription() {
        return "In-memory storage driver for development and caching";
    }
}
