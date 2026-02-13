package win.ixuni.chimera.driver.postgresql.sharding;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import win.ixuni.chimera.driver.postgresql.config.PostgresDriverConfig;

import java.nio.charset.StandardCharsets;
import java.util.zip.CRC32;

/**
 * Shard router
 * 
 * Uses a strategy similar to Redis Cluster Hash Slots
 */
@Slf4j
@RequiredArgsConstructor
public class ShardingRouter {

    private final PostgresDriverConfig config;

    /**
     * 计算对象的 Hash Slot
     */
    public int calculateHashSlot(String bucketName, String objectKey) {
        String key = bucketName + "/" + objectKey;
        CRC32 crc32 = new CRC32();
        crc32.update(key.getBytes(StandardCharsets.UTF_8));
        return (int) (crc32.getValue() % config.getTotalHashSlots());
    }

    /**
     * 获取文件切片表名
     * 
     * With declarative partitioning, all operations use the parent table t_chunk,
     * PostgreSQL automatically routes to the correct partition
     */
    public String getChunkTableName(String bucketName, String objectKey) {
        return "t_chunk";
    }

    /**
     * Get the database index for the data
     */
    public int getDataDbIndex(String bucketName, String objectKey) {
        int hashSlot = calculateHashSlot(bucketName, objectKey);
        return config.getDataDbIndex(hashSlot);
    }

    /**
     * 获取分片信息
     * 
     * 声明式分区模式下，chunkTable 固定为 t_chunk
     */
    public ShardInfo getShardInfo(String bucketName, String objectKey) {
        int hashSlot = calculateHashSlot(bucketName, objectKey);
        return ShardInfo.builder()
                .hashSlot(hashSlot)
                .dataDbIndex(config.getDataDbIndex(hashSlot))
                .tableSuffix(config.getTableSuffix(hashSlot))
                .objectTable("t_object")
                .chunkTable("t_chunk") // Declarative partitioning: use parent table
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
        private String chunkTable;
    }
}
