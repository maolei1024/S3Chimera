package win.ixuni.chimera.driver.mysql.config;

import lombok.Builder;
import lombok.Data;

import java.util.List;

/**
 * MySQL 驱动配置
 *
 * 支持分库分表配置，类似 Redis Cluster 的 Hash Slot 策略
 * 
 * 数据库配置支持两种模式：
 * 1. Simple mode: use dataUrls + dataUsername + dataPassword, shared credentials for all databases
 * 2. Independent mode: use dataSources, each database configured independently with URL, username, password
 */
@Data
@Builder
public class MysqlDriverConfig {

    /**
     * 元数据库 URL (存储 bucket、object 元数据)
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
     * 数据库 URL 列表 (存储文件切片数据) - 简单模式
     * 支持动态扩容，格式: r2dbc:mysql://host:port/chimera_data_0
     */
    private String[] dataUrls;

    /**
     * Database username - simple mode (shared by all databases)
     */
    private String dataUsername;

    /**
     * Database password - simple mode (shared by all databases)
     */
    private String dataPassword;

    /**
     * 数据源列表 - 独立模式（每个数据库独立配置）
     * 如果配置了此项，会忽略 dataUrls/dataUsername/dataPassword
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
     * 每个库的分表数量 (默认 16，支持 1-256)
     * Uses powers of 2 for convenient bitwise operations
     */
    @Builder.Default
    private int tableShardCount = 16;

    /**
     * Hash Slot 总数 (类似 Redis 的 16384 slots)
     * 默认 1024，每个数据库负责 1024/dataDbCount 个 slot
     */
    @Builder.Default
    private int totalHashSlots = 1024;

    /**
     * 切片大小 (字节)，默认 4MB
     */
    @Builder.Default
    private int chunkSize = 4 * 1024 * 1024;

    /**
     * 连接池大小 (每个数据库)
     */
    @Builder.Default
    private int poolSize = 10;

    /**
     * 并行写入数 (提高上传吞吐量)
     * Note: each concurrent task uses approximately chunkSize memory; default 2 means ~8MB max for write buffers
     */
    @Builder.Default
    private int writeConcurrency = 2;

    /**
     * 并行读取数 (提高下载吞吐量)
     * Note: each concurrent task uses approximately chunkSize memory; default 4 means ~16MB max for read buffers
     */
    @Builder.Default
    private int readConcurrency = 4;

    /**
     * 是否自动创建数据库和表结构
     */
    @Builder.Default
    private boolean autoCreateSchema = true;

    /**
     * 启动时连接验证超时（秒）
     * 如果在此时间内无法成功连接数据库，驱动将拒绝启动
     */
    @Builder.Default
    private int connectionValidationTimeout = 30;

    /**
     * 运行时最大重连次数
     * 超过此次数后将触发致命错误并退出
     */
    @Builder.Default
    private int maxReconnectAttempts = 10;

    /**
     * 重连初始等待时间（毫秒）
     */
    @Builder.Default
    private long reconnectInitialBackoff = 1000;

    /**
     * 重连最大等待时间（毫秒）
     */
    @Builder.Default
    private long reconnectMaxBackoff = 30000;

    /**
     * 获取数据库数量
     * Returns 0 to indicate single-database mode (chunk data stored in metadata database)
     */
    public int getDataDbCount() {
        if (dataSources != null && !dataSources.isEmpty()) {
            return dataSources.size();
        }
        return dataUrls != null ? dataUrls.length : 0;
    }

    /**
     * Get data source configuration for the specified index
     * 兼容简单模式和独立模式
     */
    public DataSourceConfig getDataSourceConfig(int index) {
        // 独立模式
        if (dataSources != null && !dataSources.isEmpty()) {
            if (index < dataSources.size()) {
                return dataSources.get(index);
            }
            return null;
        }
        // 简单模式
        if (dataUrls != null && index < dataUrls.length) {
            return DataSourceConfig.builder()
                    .name("data-" + index)
                    .url(dataUrls[index].trim())
                    .username(dataUsername)
                    .password(dataPassword)
                    .build();
        }
        return null;
    }

    /**
     * 计算表后缀 (00-FF)
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
        // 每个数据库负责的 slot 数量
        int slotsPerDb = totalHashSlots / getDataDbCount();
        return Math.min(hashSlot / slotsPerDb, getDataDbCount() - 1);
    }
}
