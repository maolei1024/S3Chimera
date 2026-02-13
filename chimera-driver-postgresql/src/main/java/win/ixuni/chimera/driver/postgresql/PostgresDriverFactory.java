package win.ixuni.chimera.driver.postgresql;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import win.ixuni.chimera.core.config.DriverConfig;
import win.ixuni.chimera.core.driver.DriverFactory;
import win.ixuni.chimera.core.driver.StorageDriver;

/**
 * PostgreSQL driver factory
 * <p>
 * Reactive storage driver based on R2DBC PostgreSQL, supporting:
 * - BYTEA 二进制存储
 * - JSONB 元数据支持
 * - 分库分表 (Hash Slot 策略)
 * - 自动 Schema 初始化
 */
@Slf4j
@Component
public class PostgresDriverFactory implements DriverFactory {

    public static final String DRIVER_TYPE = "postgresql";

    @Override
    public String getDriverType() {
        return DRIVER_TYPE;
    }

    @Override
    public StorageDriver createDriver(DriverConfig config) {
        log.info("Creating PostgreSQL driver instance: {}, meta-url: {}",
                config.getName(), config.getString("meta-url", ""));
        return new PostgresStorageDriverV2(config);
    }

    @Override
    public String getDescription() {
        return "R2DBC-based PostgreSQL storage driver with sharding and JSONB support";
    }
}
