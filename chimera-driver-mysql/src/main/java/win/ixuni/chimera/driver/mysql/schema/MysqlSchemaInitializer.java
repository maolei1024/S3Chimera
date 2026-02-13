package win.ixuni.chimera.driver.mysql.schema;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.driver.mysql.config.MysqlDriverConfig;

import java.util.ArrayList;
import java.util.List;

/**
 * MySQL Schema 初始化器
 *
 * 自动创建数据库表结构:
 * - 元数据库: t_bucket, t_object, t_object_metadata, t_multipart_upload,
 * t_multipart_part (单表)
 * - 数据库: t_chunk_xx (分4表，存储实际数据块)
 *
 * Design principle: shard only high-volume chunk tables; keep metadata tables as single tables for query performance
 * Uses standard SQL syntax, compatible with MySQL and H2 (MySQL mode)
 */
@Slf4j
@RequiredArgsConstructor
public class MysqlSchemaInitializer {

    private final MysqlDriverConfig config;
    private final ConnectionFactory metaConnectionFactory;
    private final List<ConnectionFactory> dataConnectionFactories;

    /**
     * Initialize all database schemas
     */
    public Mono<Void> initialize() {
        log.info("Initializing MySQL schema with {} data databases, {} tables per database",
                config.getDataDbCount(), config.getTableShardCount());

        return initializeMetaSchema()
                .then(initializeDataSchemas())
                .doOnSuccess(v -> log.info("MySQL schema initialization completed"))
                .doOnError(e -> log.error("MySQL schema initialization failed", e));
    }

    /**
     * 初始化元数据库 Schema
     */
    private Mono<Void> initializeMetaSchema() {
        log.debug("Initializing meta database schema...");

        List<String> ddlStatements = new ArrayList<>();

        // Bucket table (not sharded) - standard SQL, compatible with H2 and MySQL
        ddlStatements.add("""
                CREATE TABLE IF NOT EXISTS t_bucket (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    bucket_name VARCHAR(63) NOT NULL UNIQUE,
                    creation_date DATETIME NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                ) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """);

        // Object metadata table (single table) - optimized for listObjects performance
        ddlStatements.add("""
                CREATE TABLE IF NOT EXISTS t_object (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    bucket_name VARCHAR(63) NOT NULL,
                    object_key VARCHAR(1024) NOT NULL,
                    size BIGINT NOT NULL DEFAULT 0,
                    etag VARCHAR(64),
                    content_type VARCHAR(256) DEFAULT 'application/octet-stream',
                    storage_class VARCHAR(32) DEFAULT 'STANDARD',
                    last_modified DATETIME NOT NULL,
                    chunk_count INT NOT NULL DEFAULT 0,
                    chunk_size INT NOT NULL DEFAULT 4194304,
                    data_db_index TINYINT NOT NULL DEFAULT 0,
                    user_metadata JSON,
                    version_id VARCHAR(64),
                    delete_marker BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY uk_bucket_key (bucket_name, object_key(255)),
                    INDEX idx_bucket_key (bucket_name, object_key(255)),
                    INDEX idx_bucket_prefix (bucket_name, object_key(191))
                ) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """);

        // Object custom metadata table (single table)
        ddlStatements.add("""
                CREATE TABLE IF NOT EXISTS t_object_metadata (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    bucket_name VARCHAR(63) NOT NULL,
                    object_key VARCHAR(1024) NOT NULL,
                    meta_key VARCHAR(256) NOT NULL,
                    meta_value TEXT,
                    INDEX idx_bucket_key (bucket_name, object_key(255))
                ) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """);

        // 分片上传会话表 (单表)
        ddlStatements.add("""
                CREATE TABLE IF NOT EXISTS t_multipart_upload (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    upload_id VARCHAR(64) NOT NULL UNIQUE,
                    bucket_name VARCHAR(63) NOT NULL,
                    object_key VARCHAR(1024) NOT NULL,
                    content_type VARCHAR(256),
                    storage_class VARCHAR(32) DEFAULT 'STANDARD',
                    initiated_at DATETIME NOT NULL,
                    status TINYINT DEFAULT 0,
                    INDEX idx_bucket (bucket_name),
                    INDEX idx_status (status)
                ) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """);

        // 分片上传 Part 表 (单表)
        ddlStatements.add("""
                CREATE TABLE IF NOT EXISTS t_multipart_part (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    upload_id VARCHAR(64) NOT NULL,
                    part_number INT NOT NULL,
                    size BIGINT NOT NULL,
                    etag VARCHAR(64) NOT NULL,
                    data_db_index TINYINT NOT NULL DEFAULT 0,
                    chunk_start_id BIGINT NOT NULL DEFAULT 0,
                    chunk_count INT NOT NULL DEFAULT 0,
                    uploaded_at DATETIME NOT NULL,
                    INDEX idx_upload (upload_id),
                    INDEX idx_upload_part (upload_id, part_number)
                ) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """);

        return executeStatements(metaConnectionFactory, ddlStatements)
                .doOnSuccess(v -> log.debug("Meta database schema initialized with {} tables", 5));
    }

    /**
     * 
     * 初始化数据库 Schema (切片数据表)
     */
    private Mono<Void> initializeDataSchemas() {
        log.info("initializeDataSchemas: dataConnectionFactories.size={}, config.getDataDbCount()={}",
                dataConnectionFactories.size(), config.getDataDbCount());

        if (dataConnectionFactories.isEmpty()) {
            log.info("No separate data databases configured, using meta database for chunks");
            return initializeChunkTablesInMeta();
        }

        log.info("Creating chunk tables in {} separate data databases", dataConnectionFactories.size());
        return Flux.range(0, dataConnectionFactories.size())
                .flatMap(i -> initializeDataSchema(i, dataConnectionFactories.get(i)))
                .then();
    }

    /**
     * 初始化单个数据库的 Schema
     */
    private Mono<Void> initializeDataSchema(int dbIndex, ConnectionFactory connectionFactory) {
        log.debug("Initializing data database {} schema...", dbIndex);

        List<String> ddlStatements = new ArrayList<>();

        for (int i = 0; i < config.getTableShardCount(); i++) {
            String suffix = String.format("%02x", i);
            ddlStatements.add(buildChunkTableDDL(suffix));
        }

        return executeStatements(connectionFactory, ddlStatements)
                .doOnSuccess(v -> log.debug("Data database {} schema initialized with {} chunk tables",
                        dbIndex, config.getTableShardCount()));
    }

    /**
     * 在元数据库中创建切片表 (单库模式)
     */
    private Mono<Void> initializeChunkTablesInMeta() {
        log.debug("Creating chunk tables in meta database (single database mode)...");

        List<String> ddlStatements = new ArrayList<>();

        for (int i = 0; i < config.getTableShardCount(); i++) {
            String suffix = String.format("%02x", i);
            ddlStatements.add(buildChunkTableDDL(suffix));
        }

        return executeStatements(metaConnectionFactory, ddlStatements)
                .doOnSuccess(v -> log.debug("Chunk tables created in meta database"));
    }

    /**
     * Build shard table DDL - standard SQL compatible with H2 and MySQL
     *
     * upload_id: used for multipart uploads, NULL for regular uploads
     * part_number: 分片上传的 part 编号，普通上传为 0
     * chunk_index: 在 part 内的 chunk 序号
     *
     * Note: object_key uses prefix index (255) to avoid exceeding MySQL index length limit (3072 bytes)
     * 
     *
     * Index design:
     * - idx_object_chunk: for normal object reads, quick lookup by (bucket_name, object_key, chunk_index)
     * - idx_object_part_chunk: for post-multipart object reads, by (part_number, chunk_index)
     * 排序
     * - idx_upload_part: for multipart upload scenarios
     */
    private String buildChunkTableDDL(String suffix) {
        // Use generated column to handle NULL upload_id
        // upload_id_key: 普通上传为空字符串，分片上传为 upload_id
        return String.format("""
                CREATE TABLE IF NOT EXISTS t_chunk_%s (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    bucket_name VARCHAR(63) NOT NULL,
                    object_key VARCHAR(1024) NOT NULL,
                    upload_id VARCHAR(64),
                    part_number INT NOT NULL DEFAULT 0,
                    chunk_index INT NOT NULL,
                    chunk_data LONGBLOB NOT NULL,
                    chunk_size INT NOT NULL,
                    checksum VARCHAR(64),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    upload_id_key VARCHAR(64) GENERATED ALWAYS AS (IFNULL(upload_id, '')) STORED,
                    INDEX idx_object_chunk (bucket_name, object_key(255), chunk_index),
                    INDEX idx_object_part_chunk (bucket_name, object_key(255), part_number, chunk_index),
                    INDEX idx_upload_part (upload_id, part_number, chunk_index),
                    UNIQUE KEY uk_chunk_full (bucket_name, object_key(255), upload_id_key, part_number, chunk_index)
                ) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """, suffix);
    }

    /**
     * Execute list of DDL statements
     */
    private Mono<Void> executeStatements(ConnectionFactory connectionFactory, List<String> statements) {
        return Mono.usingWhen(
                Mono.from(connectionFactory.create()),
                connection -> Flux.fromIterable(statements)
                        .concatMap(sql -> executeStatement(connection, sql))
                        .then(),
                connection -> Mono.from(connection.close()));
    }

    /**
     * Execute a single DDL statement
     */
    private Mono<Void> executeStatement(Connection connection, String sql) {
        return Mono.from(connection.createStatement(sql).execute())
                .flatMap(result -> Mono.from(result.getRowsUpdated()))
                .then()
                .onErrorResume(e -> {
                    String message = e.getMessage();
                    // 忽略 "table already exists" 等可接受的错误
                    if (message != null && (message.contains("already exists") ||
                            message.contains("Duplicate key") ||
                            message.contains("CREATE TABLE IF NOT EXISTS"))) {
                        log.debug("DDL skipped (table exists): {}", message);
                        return Mono.empty();
                    }
                    // 其他错误视为致命错误，重新抛出
                    log.error("DDL execution failed: {}", message);
                    return Mono.error(new SchemaInitializationException("Schema DDL execution failed: " + message, e));
                });
    }

    /**
     * Schema initialization failure exception
     */
    public static class SchemaInitializationException extends RuntimeException {
        public SchemaInitializationException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
