package win.ixuni.chimera.driver.mysql.sharding;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import win.ixuni.chimera.driver.mysql.config.MysqlDriverConfig;

import java.nio.charset.StandardCharsets;
import java.util.zip.CRC32;

/**
 * Shard router
 *
 * Uses a strategy similar to Redis Cluster Hash Slots:
 * 1. Use CRC32 algorithm to compute the hash value of the key
 * 2. 对 totalHashSlots 取模得到 slot 号
 * 3. Route to the corresponding database and table by slot number
 *
 * 支持动态扩容：
 * - When scaling, only data for some slots needs to be migrated
 * - 新增数据库会自动分配 slot 范围
 */
@Slf4j
@RequiredArgsConstructor
public class ShardingRouter {

    private final MysqlDriverConfig config;

    /**
     * 计算对象的 Hash Slot
     *
     * @param bucketName bucket 名称
     * @param objectKey  对象 key
     * @return hash slot (0 ~ totalHashSlots-1)
     */
    public int calculateHashSlot(String bucketName, String objectKey) {
        String key = bucketName + "/" + objectKey;
        CRC32 crc32 = new CRC32();
        crc32.update(key.getBytes(StandardCharsets.UTF_8));
        int hashSlot = (int) (crc32.getValue() % config.getTotalHashSlots());
        log.debug(
                "[SHARDING] calculateHashSlot: bucket='{}', objectKey='{}' (len={}), combinedKey='{}', crc32={}, slot={}, suffix={}",
                bucketName, objectKey, objectKey.length(), key, crc32.getValue(), hashSlot,
                config.getTableSuffix(hashSlot));
        return hashSlot;
    }

    /**
     * 获取对象元数据表名 (单表，不分片)
     *
     * @param bucketName bucket 名称
     * @param objectKey  对象 key
     * @return 表名: t_object
     */
    public String getObjectTableName(String bucketName, String objectKey) {
        return "t_object";
    }

    /**
     * Get object custom metadata table name (single table, not sharded)
     */
    public String getObjectMetadataTableName(String bucketName, String objectKey) {
        return "t_object_metadata";
    }

    /**
     * 获取文件切片表名
     */
    public String getChunkTableName(String bucketName, String objectKey) {
        int hashSlot = calculateHashSlot(bucketName, objectKey);
        return "t_chunk_" + config.getTableSuffix(hashSlot);
    }

    /**
     * 获取分片上传会话表名 (单表，不分片)
     */
    public String getMultipartUploadTableName(String uploadId) {
        return "t_multipart_upload";
    }

    /**
     * 获取分片上传 Part 表名 (单表，不分片)
     */
    public String getMultipartPartTableName(String uploadId) {
        return "t_multipart_part";
    }

    /**
     * Get the database index for the data
     *
     * @param bucketName bucket 名称
     * @param objectKey  对象 key
     * @return database index (0 ~ dataDbCount-1)
     */
    public int getDataDbIndex(String bucketName, String objectKey) {
        int hashSlot = calculateHashSlot(bucketName, objectKey);
        return config.getDataDbIndex(hashSlot);
    }

    /**
     * 获取分片信息
     */
    public ShardInfo getShardInfo(String bucketName, String objectKey) {
        int hashSlot = calculateHashSlot(bucketName, objectKey);
        return ShardInfo.builder()
                .hashSlot(hashSlot)
                .dataDbIndex(config.getDataDbIndex(hashSlot))
                .tableSuffix(config.getTableSuffix(hashSlot))
                .objectTable("t_object") // 单表
                .objectMetadataTable("t_object_metadata") // 单表
                .chunkTable("t_chunk_" + config.getTableSuffix(hashSlot)) // chunk 保持分表
                .build();
    }

    /**
     * 分片信息
     */
    @lombok.Data
    @lombok.Builder
    public static class ShardInfo {
        private int hashSlot;
        private int dataDbIndex;
        private String tableSuffix;
        private String objectTable;
        private String objectMetadataTable;
        private String chunkTable;
    }
}
