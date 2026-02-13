package win.ixuni.chimera.driver.postgresql.config;

import lombok.Builder;
import lombok.Data;

import java.util.List;

/**
 * PostgreSQL 驱动配置
 * 
 * 支持主从数据库配置，类似 MySQL 驱动的 Hash Slot 策略
 * 
 * 数据库配置支持两种模式：
 * 1. Simple mode: use dataUrls + dataUsername + dataPassword
 * 2. Independent mode: use dataSources, each database configured separately
 */
@Data
@Builder
public class PostgresDriverConfig {

    /**
     * 元数据库 URL (存储 bucket、object 元数据)
     * 格式: r2dbc:postgresql://host:port/database
     */
    private String metaUrl;

    /**
     * Metadata database username
     */
    private String metaUsername;

    /**
     * 元数据库密码
     */
    private String metaPassword;

    /**
     * 从数据库配置列表（可选）
     * If empty or null, the metadata database stores all data
     */
    private List<DataSourceConfig> dataSources;

    /**
     * 单个数据源配置
     */
    @Data
    @Builder
    public static class DataSourceConfig {
        /** Data source name (optional, for logging) */
        private String name;
        /** 数据库 URL */
        private String url;
        /** Username */
        private String username;
        /** 密码 */
        private String password;
    }

    /**
     * 每个库的分表数量 (默认 16)
     */
    @Builder.Default
    private int tableShardCount = 16;

    /**
     * Hash Slot 总数 (默认 1024)
     */
    @Builder.Default
    private int totalHashSlots = 1024;

    /**
     * 切片大小（字节），默认 4MB
     */
    @Builder.Default
    private int chunkSize = 4 * 1024 * 1024;

    /**
     * 连接池大小
     */
    @Builder.Default
    private int poolSize = 10;

    /**
     * 并行写入数
     */
    @Builder.Default
    private int writeConcurrency = 2;

    /**
     * 并行读取数
     */
    @Builder.Default
    private int readConcurrency = 4;

    /**
     * 是否自动创建 Schema
     */
    @Builder.Default
    private boolean autoCreateSchema = true;

    /**
     * 获取从数据库数量
     * Returns 0 to indicate single-database mode
     */
    public int getDataDbCount() {
        return dataSources != null ? dataSources.size() : 0;
    }

    /**
     * Get data source configuration for the specified index
     */
    public DataSourceConfig getDataSourceConfig(int index) {
        if (dataSources != null && index < dataSources.size()) {
            return dataSources.get(index);
        }
        return null;
    }

    /**
     * 计算表后缀 (00-ff)
     */
    public String getTableSuffix(int hashSlot) {
        int tableIndex = hashSlot % tableShardCount;
        return String.format("%02x", tableIndex);
    }

    /**
     * Compute database index from hash slot
     */
    public int getDataDbIndex(int hashSlot) {
        if (getDataDbCount() <= 1) {
            return 0;
        }
        int slotsPerDb = totalHashSlots / getDataDbCount();
        return Math.min(hashSlot / slotsPerDb, getDataDbCount() - 1);
    }
}
