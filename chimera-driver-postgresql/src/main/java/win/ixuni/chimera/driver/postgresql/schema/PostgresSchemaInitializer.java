package win.ixuni.chimera.driver.postgresql.schema;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.driver.postgresql.config.PostgresDriverConfig;

import java.util.ArrayList;
import java.util.List;

/**
 * PostgreSQL Schema 初始化器
 * 
 * 自动创建数据库表结构:
 * - 元数据库: t_bucket, t_object, t_multipart_upload, t_multipart_part
 * - 数据库: t_chunk (分区父表) + t_chunk_xx (分区表)
 * 
 * Uses PostgreSQL declarative partitioning (Declarative Partitioning):
 * - All SQL operations on parent table t_chunk; PostgreSQL auto-routes to the correct partition
 * - Partition count is controlled by the tableShardCount configuration
 * - partition_key generated column hashes bucket_name + object_key for even data distribution
 */
@Slf4j
@RequiredArgsConstructor
public class PostgresSchemaInitializer {

        private final PostgresDriverConfig config;
        private final ConnectionFactory metaConnectionFactory;
        private final List<ConnectionFactory> dataConnectionFactories;

        /**
         * Initialize all database schemas
         */
        public Mono<Void> initialize() {
                log.info("初始化 PostgreSQL schema，从数据库数量: {}, 每库表数量: {}",
                                config.getDataDbCount(), config.getTableShardCount());

                return initializeMetaSchema()
                                .then(initializeDataSchemas())
                                .doOnSuccess(v -> log.info("PostgreSQL schema 初始化完成"))
                                .doOnError(e -> log.error("PostgreSQL schema 初始化失败", e));
        }

        /**
         * 初始化元数据库 Schema
         */
        private Mono<Void> initializeMetaSchema() {
                log.debug("初始化元数据库 schema...");

                List<String> ddlStatements = new ArrayList<>();

                // S3Bucket 表
                ddlStatements.add("""
                                CREATE TABLE IF NOT EXISTS t_bucket (
                                    id BIGSERIAL PRIMARY KEY,
                                    bucket_name VARCHAR(63) NOT NULL UNIQUE,
                                    creation_date TIMESTAMP NOT NULL,
                                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                                )
                                """);

                // Object metadata table - using JSONB to store custom metadata
                ddlStatements.add("""
                                CREATE TABLE IF NOT EXISTS t_object (
                                    id BIGSERIAL PRIMARY KEY,
                                    bucket_name VARCHAR(63) NOT NULL,
                                    object_key VARCHAR(1024) NOT NULL,
                                    size BIGINT NOT NULL DEFAULT 0,
                                    etag VARCHAR(64),
                                    content_type VARCHAR(256) DEFAULT 'application/octet-stream',
                                    storage_class VARCHAR(32) DEFAULT 'STANDARD',
                                    last_modified TIMESTAMP NOT NULL,
                                    chunk_count INT NOT NULL DEFAULT 0,
                                    chunk_size INT NOT NULL DEFAULT 4194304,
                                    data_db_index SMALLINT NOT NULL DEFAULT 0,
                                    user_metadata JSONB,
                                    upload_version VARCHAR(64),
                                    version_id VARCHAR(64),
                                    delete_marker BOOLEAN DEFAULT FALSE,
                                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                    UNIQUE (bucket_name, object_key)
                                )
                                """);

                // Create indexes
                ddlStatements.add("""
                                CREATE INDEX IF NOT EXISTS idx_object_bucket_key
                                ON t_object (bucket_name, object_key)
                                """);

                // 分片上传会话表
                ddlStatements.add("""
                                CREATE TABLE IF NOT EXISTS t_multipart_upload (
                                    id BIGSERIAL PRIMARY KEY,
                                    upload_id VARCHAR(64) NOT NULL UNIQUE,
                                    bucket_name VARCHAR(63) NOT NULL,
                                    object_key VARCHAR(1024) NOT NULL,
                                    content_type VARCHAR(256),
                                    storage_class VARCHAR(32) DEFAULT 'STANDARD',
                                    initiated_at TIMESTAMP NOT NULL,
                                    status SMALLINT DEFAULT 0
                                )
                                """);

                ddlStatements.add("""
                                CREATE INDEX IF NOT EXISTS idx_multipart_bucket
                                ON t_multipart_upload (bucket_name)
                                """);

                // 分片上传 Part 表
                ddlStatements.add("""
                                CREATE TABLE IF NOT EXISTS t_multipart_part (
                                    id BIGSERIAL PRIMARY KEY,
                                    upload_id VARCHAR(64) NOT NULL,
                                    part_number INT NOT NULL,
                                    size BIGINT NOT NULL,
                                    etag VARCHAR(64) NOT NULL,
                                    data_db_index SMALLINT NOT NULL DEFAULT 0,
                                    chunk_start_id BIGINT NOT NULL DEFAULT 0,
                                    chunk_count INT NOT NULL DEFAULT 0,
                                    uploaded_at TIMESTAMP NOT NULL
                                )
                                """);

                ddlStatements.add("""
                                CREATE INDEX IF NOT EXISTS idx_part_upload
                                ON t_multipart_part (upload_id)
                                """);

                ddlStatements.add("""
                                CREATE UNIQUE INDEX IF NOT EXISTS uk_part_upload_number
                                ON t_multipart_part (upload_id, part_number)
                                """);

                return executeStatements(metaConnectionFactory, ddlStatements)
                                .doOnSuccess(v -> log.debug("元数据库 schema 初始化完成"));
        }

        /**
         * 初始化从数据库 Schema
         */
        private Mono<Void> initializeDataSchemas() {
                if (dataConnectionFactories.isEmpty()) {
                        log.info("无独立从数据库，在元数据库中创建 chunk 表");
                        return initializeChunkTablesInMeta();
                }

                log.info("在 {} 个从数据库中创建 chunk 表", dataConnectionFactories.size());
                return Flux.range(0, dataConnectionFactories.size())
                                .flatMap(i -> initializeDataSchema(i, dataConnectionFactories.get(i)))
                                .then();
        }

        /**
         * Initialize a single data database - using declarative partitioning
         */
        private Mono<Void> initializeDataSchema(int dbIndex, ConnectionFactory connectionFactory) {
                List<String> ddlStatements = buildPartitionedChunkTableDDL();

                return executeStatements(connectionFactory, ddlStatements)
                                .doOnSuccess(v -> log.debug("从数据库 {} schema 初始化完成 (声明式分区 {} 个)",
                                                dbIndex, config.getTableShardCount()));
        }

        /**
         * Create chunk table in metadata database (single-DB mode) - using declarative partitioning
         */
        private Mono<Void> initializeChunkTablesInMeta() {
                List<String> ddlStatements = buildPartitionedChunkTableDDL();

                return executeStatements(metaConnectionFactory, ddlStatements)
                                .doOnSuccess(v -> log.debug("元数据库 chunk 表创建完成 (声明式分区 {} 个)",
                                                config.getTableShardCount()));
        }

        /**
         * 构建声明式分区的 chunk 表 DDL
         * 
         * Uses PostgreSQL native PARTITION BY HASH:
         * - Hash partitioning based on (bucket_name, object_key)
         * - PostgreSQL automatically computes hash values and routes to the correct partition
         * - Different objects in the same bucket are evenly distributed across partitions
         */
        private List<String> buildPartitionedChunkTableDDL() {
                List<String> ddlStatements = new ArrayList<>();
                int partitionCount = config.getTableShardCount();

                // 1. Create partition parent table (HASH partitioning based on bucket_name and object_key)
                ddlStatements.add("""
                                CREATE TABLE IF NOT EXISTS t_chunk (
                                    id BIGSERIAL,
                                    bucket_name VARCHAR(63) NOT NULL,
                                    object_key VARCHAR(1024) NOT NULL,
                                    upload_id VARCHAR(64),
                                    part_number INT NOT NULL DEFAULT 0,
                                    chunk_index INT NOT NULL,
                                    chunk_data BYTEA NOT NULL,
                                    chunk_size INT NOT NULL,
                                    checksum VARCHAR(64),
                                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                    PRIMARY KEY (id, bucket_name, object_key)
                                ) PARTITION BY HASH (bucket_name, object_key)
                                """);

                // 2. 创建分区 (MODULUS = partitionCount, REMAINDER = 0 到 partitionCount-1)
                for (int i = 0; i < partitionCount; i++) {
                        String suffix = String.format("%02x", i);
                        ddlStatements.add(String.format(
                                        "CREATE TABLE IF NOT EXISTS t_chunk_%s PARTITION OF t_chunk FOR VALUES WITH (MODULUS %d, REMAINDER %d)",
                                        suffix, partitionCount, i));
                }

                // 3. Create indexes (on parent table, automatically propagated to partitions)
                ddlStatements.add("""
                                CREATE INDEX IF NOT EXISTS idx_chunk_object
                                ON t_chunk (bucket_name, object_key, chunk_index)
                                """);

                // Index for post-multipart object reads, sorted by (part_number, chunk_index)
                ddlStatements.add("""
                                CREATE INDEX IF NOT EXISTS idx_chunk_object_part
                                ON t_chunk (bucket_name, object_key, part_number, chunk_index)
                                WHERE upload_id IS NULL
                                """);

                ddlStatements.add("""
                                CREATE INDEX IF NOT EXISTS idx_chunk_upload
                                ON t_chunk (upload_id, part_number, chunk_index)
                                WHERE upload_id IS NOT NULL
                                """);

                return ddlStatements;
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
                                        if (message != null && (message.contains("already exists") ||
                                                        message.contains("duplicate key"))) {
                                                log.debug("DDL skipped (already exists): {}", message);
                                                return Mono.empty();
                                        }
                                        log.error("DDL 执行失败: {}", message);
                                        return Mono.error(e);
                                });
        }
}
