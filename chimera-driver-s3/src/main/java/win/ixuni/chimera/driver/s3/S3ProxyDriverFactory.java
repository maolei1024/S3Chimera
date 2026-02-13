package win.ixuni.chimera.driver.s3;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import win.ixuni.chimera.core.config.DriverConfig;
import win.ixuni.chimera.core.driver.DriverFactory;
import win.ixuni.chimera.core.driver.StorageDriver;

/**
 * S3 proxy driver factory
 * <p>
 * 创建连接到标准 S3 兼容后端（MinIO、AWS S3、阿里云 OSS 等）的驱动实例。
 */
@Slf4j
@Component
public class S3ProxyDriverFactory implements DriverFactory {

    public static final String DRIVER_TYPE = "s3";

    @Override
    public String getDriverType() {
        return DRIVER_TYPE;
    }

    @Override
    public StorageDriver createDriver(DriverConfig config) {
        log.info("Creating S3 proxy driver instance: {}", config.getName());
        return new S3ProxyStorageDriverV2(config);
    }

    @Override
    public String getDescription() {
        return "S3 proxy driver for MinIO, AWS S3, and other S3-compatible backends";
    }
}
