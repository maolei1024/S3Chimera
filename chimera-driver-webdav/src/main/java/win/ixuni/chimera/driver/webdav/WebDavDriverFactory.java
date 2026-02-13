package win.ixuni.chimera.driver.webdav;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import win.ixuni.chimera.core.config.DriverConfig;
import win.ixuni.chimera.core.driver.DriverFactory;
import win.ixuni.chimera.core.driver.StorageDriver;

/**
 * WebDAV driver factory
 * <p>
 * 创建连接到 WebDAV 服务器（坚果云、Nextcloud 等）的驱动实例。
 */
@Slf4j
@Component
public class WebDavDriverFactory implements DriverFactory {

    public static final String DRIVER_TYPE = "webdav";

    @Override
    public String getDriverType() {
        return DRIVER_TYPE;
    }

    @Override
    public StorageDriver createDriver(DriverConfig config) {
        log.info("Creating WebDAV driver instance: {}", config.getName());
        return new WebDavStorageDriverV2(config);
    }

    @Override
    public String getDescription() {
        return "WebDAV driver for Nextcloud, Jianguoyun, and other WebDAV servers";
    }
}
