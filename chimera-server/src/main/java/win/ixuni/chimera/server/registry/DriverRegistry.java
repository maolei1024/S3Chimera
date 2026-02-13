package win.ixuni.chimera.server.registry;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.config.ChimeraProperties;
import win.ixuni.chimera.core.config.DriverConfig;
import win.ixuni.chimera.core.driver.DriverFactory;
import win.ixuni.chimera.core.driver.StorageDriver;
import win.ixuni.chimera.core.exception.DriverNotFoundException;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 驱动注册表
 * <p>
 * Loads and manages all driver instances from configuration.
 * Supports multiple instances of the same driver type.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DriverRegistry {

    private final ChimeraProperties properties;
    private final List<DriverFactory> driverFactories;

    /**
     * Driver instance mapping: name -> driver
     */
    private final Map<String, StorageDriver> drivers = new ConcurrentHashMap<>();

    /**
     * Driver factory mapping: type -> factory
     */
    private final Map<String, DriverFactory> factoryMap = new ConcurrentHashMap<>();

    @PostConstruct
    public void initialize() {
        log.info("Initializing driver registry...");
        log.info("Found {} driver factories", driverFactories.size());

        // Build factory mapping
        for (DriverFactory factory : driverFactories) {
            factoryMap.put(factory.getDriverType(), factory);
            log.info("Registered driver factory: {} - {}", factory.getDriverType(), factory.getDescription());
        }

        // 根据配置创建驱动实例
        for (DriverConfig config : properties.getDrivers()) {
            if (!config.isEnabled()) {
                log.info("Driver '{}' is disabled, skipping", config.getName());
                continue;
            }

            DriverFactory factory = factoryMap.get(config.getType());
            if (factory == null) {
                log.error("Unknown driver type '{}' for driver '{}'", config.getType(), config.getName());
                continue;
            }

            try {
                StorageDriver driver = factory.createDriver(config);
                driver.initialize().block();
                drivers.put(config.getName(), driver);
                log.info("Created driver instance: {} (type: {})", config.getName(), config.getType());
            } catch (Exception e) {
                log.error("Failed to create driver '{}': {}", config.getName(), e.getMessage(), e);
            }
        }

        log.info("Driver registry initialized with {} drivers", drivers.size());
    }

    @PreDestroy
    public void shutdown() {
        log.info("Shutting down driver registry...");
        Flux.fromIterable(drivers.values())
                .flatMap(driver -> driver.shutdown()
                        .doOnSuccess(v -> log.info("Driver '{}' shutdown complete", driver.getDriverName()))
                        .onErrorResume(e -> {
                            log.error("Error shutting down driver '{}': {}", driver.getDriverName(), e.getMessage());
                            return Mono.empty();
                        }))
                .blockLast();
        log.info("Driver registry shutdown complete");
    }

    /**
     * 获取驱动实例
     *
     * @param name 驱动名称
     * @return driver instance
     */
    public StorageDriver getDriver(String name) {
        StorageDriver driver = drivers.get(name);
        if (driver == null) {
            throw new DriverNotFoundException(name);
        }
        return driver;
    }

    /**
     * 获取驱动实例（可选）
     *
     * @param name 驱动名称
     * @return 驱动实例Optional
     */
    public Optional<StorageDriver> findDriver(String name) {
        return Optional.ofNullable(drivers.get(name));
    }

    /**
     * Get all driver instances
     *
     * @return 驱动实例集合
     */
    public Map<String, StorageDriver> getAllDrivers() {
        return Map.copyOf(drivers);
    }

    /**
     * 检查驱动是否存在
     *
     * @param name 驱动名称
     * @return 是否存在
     */
    public boolean hasDriver(String name) {
        return drivers.containsKey(name);
    }
}
