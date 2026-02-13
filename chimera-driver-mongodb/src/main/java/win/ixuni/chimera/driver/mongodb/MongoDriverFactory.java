package win.ixuni.chimera.driver.mongodb;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import win.ixuni.chimera.core.config.DriverConfig;
import win.ixuni.chimera.core.driver.DriverFactory;
import win.ixuni.chimera.core.driver.StorageDriver;

/**
 * MongoDB driver factory
 * <p>
 * Reactive storage driver based on MongoDB Reactive Streams, supporting:
 * - BSON Binary 二进制存储
 * - 分片存储
 * - 自动 Schema 初始化
 */
@Slf4j
@Component
public class MongoDriverFactory implements DriverFactory {

    public static final String DRIVER_TYPE = "mongodb";

    @Override
    public String getDriverType() {
        return DRIVER_TYPE;
    }

    @Override
    public StorageDriver createDriver(DriverConfig config) {
        log.info("Creating MongoDB driver instance: {}, uri: {}",
                config.getName(), config.getString("uri", "mongodb://localhost:27017"));
        return new MongoStorageDriverV2(config);
    }

    @Override
    public String getDescription() {
        return "MongoDB Reactive Streams storage driver with BSON binary support";
    }
}
