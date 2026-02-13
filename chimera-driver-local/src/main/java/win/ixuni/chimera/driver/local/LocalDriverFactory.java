package win.ixuni.chimera.driver.local;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import win.ixuni.chimera.core.config.DriverConfig;
import win.ixuni.chimera.core.driver.DriverFactory;
import win.ixuni.chimera.core.driver.StorageDriver;

/**
 * 本地文件系统驱动工厂
 */
@Slf4j
@Component
public class LocalDriverFactory implements DriverFactory {

    public static final String DRIVER_TYPE = "local";

    @Override
    public String getDriverType() {
        return DRIVER_TYPE;
    }

    @Override
    public StorageDriver createDriver(DriverConfig config) {
        log.info("Creating local filesystem driver instance: {}", config.getName());
        return new LocalStorageDriverV2(config);
    }

    @Override
    public String getDescription() {
        return "Local filesystem storage driver";
    }
}
