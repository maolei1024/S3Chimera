package win.ixuni.chimera.core.config;

import lombok.Data;

/**
 * Bucket routing rule
 * <p>
 * Defines the mapping from bucket name patterns to driver instances.
 */
@Data
public class BucketRoutingRule {

    /**
     * Bucket name matching pattern
     * <p>
     * Supports wildcards:
     * - * matches any characters
     * - ? matches a single character
     * <p>
     * Examples:
     * - "cache-*" matches all buckets starting with cache-
     * - "backup-202?" matches backup-2020, backup-2021, etc.
     * - "my-bucket" exact match
     */
    private String pattern;

    /**
     * Target driver instance name
     */
    private String driver;

    /**
     * Rule priority (lower number = higher priority)
     */
    private int priority = 100;
}
