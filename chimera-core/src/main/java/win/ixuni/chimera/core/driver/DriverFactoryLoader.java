package win.ixuni.chimera.core.driver;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ServiceLoader;

/**
 * Driver factory loader
 * <p>
 * Uses Java SPI (ServiceLoader) to automatically discover DriverFactory implementations on the classpath.
 * Third-party drivers only need to declare themselves in META-INF/services to be auto-discovered.
 * <p>
 * Usage example:
 * 
 * <pre>
 * List&lt;DriverFactory&gt; factories = DriverFactoryLoader.load();
 * factories.forEach(f -&gt; System.out.println("Found: " + f.getDriverType()));
 * </pre>
 */
@Slf4j
public final class DriverFactoryLoader {

    private DriverFactoryLoader() {
        // Utility class, not instantiable
    }

    /**
     * Load all DriverFactory implementations via SPI
     *
     * @return list of discovered factories
     */
    public static List<DriverFactory> load() {
        return load(Thread.currentThread().getContextClassLoader());
    }

    /**
     * Load all DriverFactory implementations via SPI
     *
     * @param classLoader class loader
     * @return list of discovered factories
     */
    public static List<DriverFactory> load(ClassLoader classLoader) {
        ServiceLoader<DriverFactory> loader = ServiceLoader.load(DriverFactory.class, classLoader);
        List<DriverFactory> factories = new ArrayList<>();

        for (DriverFactory factory : loader) {
            factories.add(factory);
            log.info("Discovered driver factory via SPI: {} - {}",
                    factory.getDriverType(), factory.getDescription());
        }

        if (factories.isEmpty()) {
            log.warn("No DriverFactory implementations found via SPI");
        } else {
            log.info("Loaded {} driver factories via SPI", factories.size());
        }

        return Collections.unmodifiableList(factories);
    }

    /**
     * Refresh and reload all factories
     *
     * @return list of discovered factories
     */
    public static List<DriverFactory> reload() {
        log.info("Reloading driver factories via SPI...");
        return load();
    }
}
