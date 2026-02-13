package win.ixuni.chimera.test.s3.crossdriver;

import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.ActiveProfiles;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.*;
import win.ixuni.chimera.server.ChimeraServerApplication;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 跨驱动矩阵测试 (完整版)
 * <p>
 * Auto-generates all driver combinations and randomly samples for P0/P1 testing.
 */
@SpringBootTest(classes = ChimeraServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("crossdriver-matrix")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class CrossDriverMatrixTest {

    private static final Logger log = LoggerFactory.getLogger(CrossDriverMatrixTest.class);

    @LocalServerPort
    private int port;

    @Autowired
    private CrossDriverProperties properties;

    private S3Client s3Client;
    private DriverCombinationPool combinationPool;
    private List<DriverCombination> selectedCombinations;
    private String testRunId;

    @BeforeAll
    void setup() {
        // 1. 初始化 S3 客户端
        S3Configuration s3Config = S3Configuration.builder()
                .pathStyleAccessEnabled(true)
                .checksumValidationEnabled(false)
                .chunkedEncodingEnabled(false)
                .build();

        s3Client = S3Client.builder()
                .endpointOverride(URI.create("http://localhost:" + port))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("test-access-key", "test-secret-key")))
                .serviceConfiguration(s3Config)
                .build();

        // 2. 初始化驱动组合池
        combinationPool = new DriverCombinationPool(properties.getDrivers());

        // 3. 随机抽取测试组合
        int pickCount = properties.getCrossdriver().getPickCount();
        selectedCombinations = combinationPool.pickRandom(pickCount);

        // 生成本次测试运行ID
        testRunId = UUID.randomUUID().toString().substring(0, 8);

        log.info("=== Cross-Driver Matrix Test Initialized ===");
        log.info("Total drivers: {}", combinationPool.getDriverCount());
        log.info("Total combinations: {}", combinationPool.getTotalCombinationCount());
        log.info("Selected {} combinations for testing:", selectedCombinations.size());
        selectedCombinations.forEach(c -> log.info(" - {}", c.getName()));
    }

    @AfterAll
    void tearDown() {
        cleanupAllBuckets();
        if (s3Client != null) {
            s3Client.close();
        }
    }

    /**
     * 提供测试组合源
     */
    Stream<DriverCombination> getCombinations() {
        return selectedCombinations.stream();
    }

    /**
     * P0: 标准跨驱动复制测试 (copyObject)
     */
    @ParameterizedTest(name = "[P0] Standard Copy: {0}")
    @MethodSource("getCombinations")
    @Order(10)
    void testStandardCopy(DriverCombination combo) {
        String srcBucket = combo.getSourceBucket(testRunId);
        String dstBucket = combo.getTargetBucket(testRunId);
        String key = "std-copy-" + UUID.randomUUID();
        String content = "Standard copy test content for " + combo.getName();

        log.info("[P0] Testing standard copy: {} -> {}", srcBucket, dstBucket);

        // 1. 确保 Buckets 存在
        ensureBucketExists(srcBucket);
        ensureBucketExists(dstBucket);

        try {
            // 2. 上传源对象 (带元数据)
            s3Client.putObject(b -> b.bucket(srcBucket).key(key)
                    .contentType("text/plain")
                    .metadata(Map.of("author", "tester")),
                    RequestBody.fromString(content));

            // 3. 执行跨驱动 CopyObject
            CopyObjectResponse copyRes = s3Client.copyObject(b -> b
                    .sourceBucket(srcBucket)
                    .sourceKey(key)
                    .destinationBucket(dstBucket)
                    .destinationKey(key));

            assertNotNull(copyRes.copyObjectResult());
            assertNotNull(copyRes.copyObjectResult().eTag());

            // 4. 验证目标对象内容
            var getRes = s3Client.getObjectAsBytes(b -> b.bucket(dstBucket).key(key));
            assertEquals(content, getRes.asString(StandardCharsets.UTF_8));

            // 5. 验证元数据保留
            var headRes = s3Client.headObject(b -> b.bucket(dstBucket).key(key));
            assertEquals("text/plain", headRes.contentType());
            assertEquals("tester", headRes.metadata().get("author"));

            log.info("[P0] Standard copy passed: {}", combo.getName());

        } finally {
            // 清理对象 (Bucket 在 AfterAll 清理)
            deleteObjectQuietly(srcBucket, key);
            deleteObjectQuietly(dstBucket, key);
        }
    }

    /**
     * P1: 大文件跨驱动复制测试 (100MB)
     */
    @ParameterizedTest(name = "[P1] Large File Copy: {0}")
    @MethodSource("getCombinations")
    @Order(20)
    void testLargeFileCopy(DriverCombination combo) {
        long size = properties.getCrossdriver().getLargeFileSize();
        String srcBucket = combo.getSourceBucket(testRunId);
        String dstBucket = combo.getTargetBucket(testRunId);
        String key = "large-copy-" + UUID.randomUUID();

        log.info("[P1] Testing large file copy ({} bytes): {} -> {}", size, srcBucket, dstBucket);

        // 1. 确保 Buckets 存在
        ensureBucketExists(srcBucket);
        ensureBucketExists(dstBucket);

        // 2. 生成随机大文件
        byte[] data = TestDataGenerator.generateRandomBytes((int) size); // Simplified: use memory directly, mind heap size
        String originalMd5 = TestDataGenerator.calculateMD5(data);

        try {
            // 3. 上传源对象
            long startUpload = System.currentTimeMillis();
            s3Client.putObject(b -> b.bucket(srcBucket).key(key), RequestBody.fromBytes(data));
            log.info("Upload took: {} ms", System.currentTimeMillis() - startUpload);

            // 4. 执行跨驱动 CopyObject
            long startCopy = System.currentTimeMillis();
            s3Client.copyObject(b -> b
                    .sourceBucket(srcBucket)
                    .sourceKey(key)
                    .destinationBucket(dstBucket)
                    .destinationKey(key));
            log.info("Copy took: {} ms", System.currentTimeMillis() - startCopy);

            // 5. 下载并验证完整性
            long startDownload = System.currentTimeMillis();
            byte[] downloaded = s3Client.getObjectAsBytes(b -> b.bucket(dstBucket).key(key)).asByteArray();
            log.info("Download took: {} ms", System.currentTimeMillis() - startDownload);

            assertEquals(data.length, downloaded.length, "Size mismatch");
            String downloadedMd5 = TestDataGenerator.calculateMD5(downloaded);
            assertEquals(originalMd5, downloadedMd5, "MD5 mismatch");

            log.info("[P1] Large file copy passed: {}", combo.getName());

        } finally {
            deleteObjectQuietly(srcBucket, key);
            deleteObjectQuietly(dstBucket, key);
        }
    }

    // ==================== 辅助方法 ====================

    private void ensureBucketExists(String bucketName) {
        try {
            s3Client.headBucket(b -> b.bucket(bucketName));
        } catch (NoSuchBucketException e) {
            try {
                s3Client.createBucket(b -> b.bucket(bucketName));
            } catch (Exception createEx) {
                // 可能并发创建，忽略
            }
        } catch (Exception e) {
            // 忽略其他错误，尝试创建
            try {
                s3Client.createBucket(b -> b.bucket(bucketName));
            } catch (Exception ignored) {
            }
        }
    }

    private void deleteObjectQuietly(String bucket, String key) {
        try {
            s3Client.deleteObject(b -> b.bucket(bucket).key(key));
        } catch (Exception e) {
            log.warn("Failed to delete object {}/{}: {}", bucket, key, e.getMessage());
        }
    }

    private void cleanupAllBuckets() {
        if (selectedCombinations == null)
            return;

        // Collect all involved buckets
        selectedCombinations.forEach(combo -> {
            cleanupBucket(combo.getSourceBucket(testRunId));
            cleanupBucket(combo.getTargetBucket(testRunId));
        });
    }

    private void cleanupBucket(String bucketName) {
        try {
            var list = s3Client.listObjectsV2(b -> b.bucket(bucketName));
            for (S3Object obj : list.contents()) {
                deleteObjectQuietly(bucketName, obj.key());
            }
            s3Client.deleteBucket(b -> b.bucket(bucketName));
        } catch (Exception e) {
            log.warn("Cleanup failed for bucket {}: {}", bucketName, e.getMessage());
        }
    }
}
