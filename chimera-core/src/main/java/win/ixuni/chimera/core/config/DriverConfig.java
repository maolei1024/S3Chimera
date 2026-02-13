package win.ixuni.chimera.core.config;

import lombok.Data;

import java.util.HashMap;
import java.util.Map;

/**
 * Driver configuration
 * <p>
 * Generic driver configuration structure. Different drivers store their specific settings in properties.
 */
@Data
public class DriverConfig {

    /**
     * Driver instance name (unique identifier)
     */
    private String name;

    /**
     * Driver type (memory, sql, webdav, nfs, etc.)
     */
    private String type;

    /**
     * Whether enabled
     */
    private boolean enabled = true;

    /**
     * Driver-specific configuration
     */
    private Map<String, Object> properties = new HashMap<>();

    /**
     * Get a configuration value
     */
    @SuppressWarnings("unchecked")
    public <T> T getProperty(String key, T defaultValue) {
        Object value = properties.get(key);
        if (value == null) {
            return defaultValue;
        }
        return (T) value;
    }

    /**
     * Get a string configuration value
     */
    public String getString(String key, String defaultValue) {
        Object value = properties.get(key);
        return value != null ? value.toString() : defaultValue;
    }

    /**
     * Get an integer configuration value
     */
    public Integer getInt(String key, Integer defaultValue) {
        Object value = properties.get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        return Integer.parseInt(value.toString());
    }

    /**
     * Get a long integer configuration value
     */
    public Long getLong(String key, Long defaultValue) {
        Object value = properties.get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        return Long.parseLong(value.toString());
    }

    /**
     * Get a boolean configuration value
     */
    public Boolean getBoolean(String key, Boolean defaultValue) {
        Object value = properties.get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        return Boolean.parseBoolean(value.toString());
    }
}
