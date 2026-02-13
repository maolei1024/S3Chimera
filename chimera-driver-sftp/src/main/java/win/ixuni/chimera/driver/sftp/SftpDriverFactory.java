package win.ixuni.chimera.driver.sftp;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import win.ixuni.chimera.core.config.DriverConfig;
import win.ixuni.chimera.core.driver.DriverFactory;
import win.ixuni.chimera.core.driver.StorageDriver;

/**
 * SFTP storage driver factory
 */
@Slf4j
@Component
public class SftpDriverFactory implements DriverFactory {

    public static final String DRIVER_TYPE = "sftp";

    @Override
    public String getDriverType() {
        return DRIVER_TYPE;
    }

    @Override
    public StorageDriver createDriver(DriverConfig config) {
        log.info("Creating SFTP driver instance: {}", config.getName());
        return new SftpStorageDriverV2(config);
    }

    @Override
    public String getDescription() {
        return "SFTP storage driver - connect to SFTP servers via SSH";
    }
}
