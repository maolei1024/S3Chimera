package win.ixuni.chimera.driver.mongodb.schema;

import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoDatabase;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.driver.mongodb.config.MongoDriverConfig;
import win.ixuni.chimera.driver.mongodb.context.MongoDriverContext;

import java.util.List;

/**
 * MongoDB Schema 初始化器
 * 
 * Automatically creates collections and indexes:
 * - 元数据库: buckets, objects, multipart_uploads, multipart_parts
 * - 数据库: chunks
 */
@Slf4j
@RequiredArgsConstructor
public class MongoSchemaInitializer {

        private final MongoDriverConfig config;
        private final MongoClient metaClient;
        private final List<MongoClient> dataClients;

        /**
         * Initialize all database schemas
         */
        public Mono<Void> initialize() {
                log.info("初始化 MongoDB schema，从数据库数量: {}", config.getDataDbCount());

                return initializeMetaSchema()
                                .then(initializeDataSchemas())
                                .doOnSuccess(v -> log.info("MongoDB schema 初始化完成"))
                                .doOnError(e -> log.error("MongoDB schema 初始化失败", e));
        }

        /**
         * 初始化元数据库 Schema
         */
        private Mono<Void> initializeMetaSchema() {
                log.debug("初始化元数据库 schema...");
                MongoDatabase db = metaClient.getDatabase(config.getMetaDatabase());

                return Mono.when(
                                // buckets collection: unique index on bucketName
                                createIndexSafe(db, MongoDriverContext.COLLECTION_BUCKETS,
                                                Indexes.ascending("bucketName"),
                                                new IndexOptions().unique(true).name("idx_bucket_name")),

                                // objects collection: bucket+key compound index
                                createIndexSafe(db, MongoDriverContext.COLLECTION_OBJECTS,
                                                Indexes.compoundIndex(
                                                                Indexes.ascending("bucketName"),
                                                                Indexes.ascending("objectKey")),
                                                new IndexOptions().unique(true).name("idx_bucket_object")),

                                // objects collection: prefix query index
                                createIndexSafe(db, MongoDriverContext.COLLECTION_OBJECTS,
                                                Indexes.ascending("bucketName"),
                                                new IndexOptions().name("idx_bucket")),

                                // multipart_uploads 集合
                                createIndexSafe(db, MongoDriverContext.COLLECTION_MULTIPART_UPLOADS,
                                                Indexes.ascending("uploadId"),
                                                new IndexOptions().unique(true).name("idx_upload_id")),

                                createIndexSafe(db, MongoDriverContext.COLLECTION_MULTIPART_UPLOADS,
                                                Indexes.ascending("bucketName"),
                                                new IndexOptions().name("idx_bucket")),

                                // multipart_parts 集合
                                createIndexSafe(db, MongoDriverContext.COLLECTION_MULTIPART_PARTS,
                                                Indexes.compoundIndex(
                                                                Indexes.ascending("uploadId"),
                                                                Indexes.ascending("partNumber")),
                                                new IndexOptions().unique(true).name("idx_upload_part")))
                                .doOnSuccess(v -> log.debug("元数据库 schema 初始化完成"));
        }

        /**
         * 初始化从数据库 Schema
         */
        private Mono<Void> initializeDataSchemas() {
                if (dataClients.isEmpty()) {
                        log.info("无独立从数据库，在元数据库中创建 chunks 集合");
                        return initializeChunksInMeta();
                }

                log.info("在 {} 个从数据库中创建 chunks 集合", dataClients.size());
                return Flux.range(0, dataClients.size())
                                .flatMap(this::initializeDataSchema)
                                .then();
        }

        /**
         * 初始化单个从数据库
         */
        private Mono<Void> initializeDataSchema(int index) {
                MongoDriverConfig.DataSourceConfig dsConfig = config.getDataSourceConfig(index);
                MongoDatabase db = dataClients.get(index).getDatabase(dsConfig.getDatabase());

                return createChunkIndexes(db)
                                .doOnSuccess(v -> log.debug("从数据库 {} schema 初始化完成", index));
        }

        /**
         * 在元数据库创建 chunks 集合（单库模式）
         */
        private Mono<Void> initializeChunksInMeta() {
                MongoDatabase db = metaClient.getDatabase(config.getMetaDatabase());
                return createChunkIndexes(db);
        }

        /**
         * Create chunks collection indexes
         */
        private Mono<Void> createChunkIndexes(MongoDatabase db) {
                return Mono.when(
                                // Index for querying by object
                                createIndexSafe(db, MongoDriverContext.COLLECTION_CHUNKS,
                                                Indexes.compoundIndex(
                                                                Indexes.ascending("bucketName"),
                                                                Indexes.ascending("objectKey"),
                                                                Indexes.ascending("chunkIndex")),
                                                new IndexOptions().name("idx_object_chunk")),

                                // For post-multipart object reads, sorted by (partNumber, chunkIndex)
                                createIndexSafe(db, MongoDriverContext.COLLECTION_CHUNKS,
                                                Indexes.compoundIndex(
                                                                Indexes.ascending("bucketName"),
                                                                Indexes.ascending("objectKey"),
                                                                Indexes.ascending("partNumber"),
                                                                Indexes.ascending("chunkIndex")),
                                                new IndexOptions().name("idx_object_part_chunk")),

                                // Multipart upload query index
                                createIndexSafe(db, MongoDriverContext.COLLECTION_CHUNKS,
                                                Indexes.compoundIndex(
                                                                Indexes.ascending("uploadId"),
                                                                Indexes.ascending("partNumber"),
                                                                Indexes.ascending("chunkIndex")),
                                                new IndexOptions().name("idx_upload_part_chunk")),

                                // Unique constraint
                                createIndexSafe(db, MongoDriverContext.COLLECTION_CHUNKS,
                                                Indexes.compoundIndex(
                                                                Indexes.ascending("bucketName"),
                                                                Indexes.ascending("objectKey"),
                                                                Indexes.ascending("uploadId"),
                                                                Indexes.ascending("partNumber"),
                                                                Indexes.ascending("chunkIndex")),
                                                new IndexOptions().unique(true).name("uk_chunk")));
        }

        /**
         * Safely create index (ignore if already exists)
         */
        private Mono<String> createIndexSafe(MongoDatabase db, String collectionName,
                        org.bson.conversions.Bson keys, IndexOptions options) {
                return Mono.from(db.getCollection(collectionName).createIndex(keys, options))
                                .onErrorResume(e -> {
                                        String msg = e.getMessage();
                                        if (msg != null && (msg.contains("already exists") ||
                                                        msg.contains("IndexOptionsConflict"))) {
                                                log.debug("Index already exists: {}.{}", collectionName, options.getName());
                                                return Mono.just(options.getName());
                                        }
                                        return Mono.error(e);
                                });
        }
}
