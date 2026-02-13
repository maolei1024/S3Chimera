package win.ixuni.chimera.driver.mongodb.config;

import lombok.Builder;
import lombok.Data;

import java.util.List;

/**
 * MongoDB 驱动配置
 * 
 * 支持主从数据库配置:
 * 1. 元数据库 (metaUri): 必需，存储元数据和 bucket 信息
 * 2. Data databases (dataUris): optional, for storing actual data chunks
 * 3. Fallback: when data databases are not configured, the metadata database stores all data
 */
@Data
@Builder
public class MongoDriverConfig {

    /**
     * 元数据库连接 URI
     * 格式: mongodb://host:port
     */
    private String metaUri;

    /**
     * 元数据库名称
     */
    @Builder.Default
    private String metaDatabase = "chimera_meta";

    /**
     * Metadata database username (optional)
     */
    private String metaUsername;

    /**
     * 元数据库密码（可选）
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
        /** MongoDB 连接 URI */
        private String uri;
        /** 数据库名称 */
        private String database;
        /** Username (optional) */
        private String username;
        /** 密码（可选） */
        private String password;
    }

    /**
     * 分块大小（字节），默认 4MB
     * MongoDB single document limit is 16MB; use smaller chunks to avoid issues
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
     * Whether to automatically create schema (collections and indexes)
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
     * Whether to use single-database mode (metadata database also stores data)
     */
    public boolean isSingleDatabaseMode() {
        return dataSources == null || dataSources.isEmpty();
    }
}
