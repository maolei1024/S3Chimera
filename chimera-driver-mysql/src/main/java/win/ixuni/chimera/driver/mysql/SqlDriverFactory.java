package win.ixuni.chimera.driver.mysql;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import win.ixuni.chimera.core.config.DriverConfig;
import win.ixuni.chimera.core.driver.DriverFactory;
import win.ixuni.chimera.core.driver.StorageDriver;

/**
 * MySQL驱动工厂
 * <p>
 * Reactive MySQL storage driver based on R2DBC, supporting:
 * - 分库分表 (Hash Slot 策略)
 * - 文件切片存储
 * - 动态扩容
 * - 自动 DDL 创建
 */
@Slf4j
@Component
public class SqlDriverFactory implements DriverFactory {

    public static final String DRIVER_TYPE = "mysql";

    @Override
    public String getDriverType() {
        return DRIVER_TYPE;
    }

    @Override
    public StorageDriver createDriver(DriverConfig config) {
        log.info("Creating MySQL driver V2 instance: {}, url: {}",
                config.getName(), config.getString("url", ""));
        return new MysqlStorageDriverV2(config);
    }

    @Override
    public String getDescription() {
        return "R2DBC-based MySQL storage driver with sharding and chunking support";
    }
}
