package win.ixuni.chimera.server.routing;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import win.ixuni.chimera.core.config.BucketRoutingRule;
import win.ixuni.chimera.core.config.ChimeraProperties;
import win.ixuni.chimera.core.driver.StorageDriver;
import win.ixuni.chimera.core.driver.StorageDriverV2;
import win.ixuni.chimera.core.exception.DriverNotFoundException;
import win.ixuni.chimera.server.registry.DriverRegistry;

import java.util.Comparator;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Bucket router
 * <p>
 * Routes bucket requests to the corresponding driver instance based on configured routing rules.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class BucketRouter {

    private final ChimeraProperties properties;
    private final DriverRegistry driverRegistry;

    /**
     * Get the corresponding V2 driver by bucket name
     *
     * @param bucketName Bucket名称
     * @return 匹配的驱动实例
     */
    public StorageDriverV2 route(String bucketName) {
        StorageDriver driver = routeToDriver(bucketName);
        if (driver instanceof StorageDriverV2 v2) {
            return v2;
        }
        throw new IllegalStateException("Driver for bucket '" + bucketName + "' is not StorageDriverV2");
    }

    /**
     * Internally routes to the base StorageDriver
     */
    private StorageDriver routeToDriver(String bucketName) {
        // Routing rules sorted by priority
        List<BucketRoutingRule> rules = properties.getRouting().getBuckets().stream()
                .sorted(Comparator.comparingInt(BucketRoutingRule::getPriority))
                .toList();

        // 遍历规则寻找匹配
        for (BucketRoutingRule rule : rules) {
            if (matchPattern(bucketName, rule.getPattern())) {
                log.debug("Bucket '{}' matched rule pattern '{}', routing to driver '{}'",
                        bucketName, rule.getPattern(), rule.getDriver());
                return driverRegistry.getDriver(rule.getDriver());
            }
        }

        // Use default driver
        String defaultDriver = properties.getRouting().getDefaultDriver();
        if (defaultDriver != null && !defaultDriver.isEmpty()) {
            log.debug("Bucket '{}' using default driver '{}'", bucketName, defaultDriver);
            return driverRegistry.getDriver(defaultDriver);
        }

        throw new DriverNotFoundException("No driver configured for bucket: " + bucketName);
    }

    /**
     * 获取默认驱动
     *
     * @return 默认驱动实例
     */
    public StorageDriverV2 getDefaultDriver() {
        String defaultDriver = properties.getRouting().getDefaultDriver();
        if (defaultDriver == null || defaultDriver.isEmpty()) {
            throw new DriverNotFoundException("No default driver configured");
        }
        StorageDriver driver = driverRegistry.getDriver(defaultDriver);
        if (driver instanceof StorageDriverV2 v2) {
            return v2;
        }
        throw new IllegalStateException("Default driver is not StorageDriverV2");
    }

    /**
     * 匹配通配符模式
     */
    private boolean matchPattern(String text, String pattern) {
        String regex = pattern
                .replace(".", "\\.")
                .replace("*", ".*")
                .replace("?", ".");
        return Pattern.matches("^" + regex + "$", text);
    }

    /**
     * 获取默认驱动名称
     */
    public String getDefaultDriverName() {
        if (properties.getRouting() == null) {
            return null;
        }
        return properties.getRouting().getDefaultDriver();
    }

    /**
     * Get all driver names referenced in routing rules
     */
    public java.util.Set<String> getRoutingRuleDriverNames() {
        if (properties.getRouting() == null || properties.getRouting().getBuckets() == null) {
            return java.util.Collections.emptySet();
        }
        return properties.getRouting().getBuckets().stream()
                .map(BucketRoutingRule::getDriver)
                .collect(java.util.stream.Collectors.toSet());
    }
}
