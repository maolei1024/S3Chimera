package win.ixuni.chimera.test.s3;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.*;
import win.ixuni.chimera.server.ChimeraServerApplication;
import win.ixuni.chimera.test.s3.util.DataIntegrityAssert;
import win.ixuni.chimera.test.s3.util.TestDataGenerator;

import java.net.URI;
import java.util.Arrays;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * S3 批量上传测试
 * <p>
 * 测试大批量小文件的上传、验证和列表功能。
 * Focuses on verifying actual content integrity, not just the absence of errors.
 * <p>
 * 运行方式：
 * - ./gradlew :chimera-test-s3:test --tests "*BulkUpload*" -Pdriver=memory
 * - ./gradlew :chimera-test-s3:test --tests "*BulkUpload*" -Pdriver=local
 */
@SpringBootTest(classes = ChimeraServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ExtendWith(SkipOnNotSupportedExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Timeout(value = 120, unit = TimeUnit.SECONDS)
public class S3BulkUploadTest {

    @LocalServerPort
    private int port;

    private S3Client s3Client;

    // Use unique bucket name to avoid conflicts with other tests
    private final String BUCKET = "bulk-upload-test-" + UUID.randomUUID().toString().substring(0, 8);

    // CI-friendly test parameters
    private static final int BULK_FILE_COUNT = 50;
    private static final int SMALL_FILE_SIZE = 1024; // 1KB
    private static final int MIN_MIXED_SIZE = 100; // 100B
    private static final int MAX_MIXED_SIZE = 100 * 1024; // 100KB

    @BeforeAll
    void setup() {
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

        // 创建测试 bucket
        s3Client.createBucket(b -> b.bucket(BUCKET));
    }

    @AfterAll
    void tearDown() {
        // Clean up all objects and buckets
        try {
            ListObjectsV2Response listResponse = s3Client.listObjectsV2(b -> b.bucket(BUCKET));
            for (S3Object obj : listResponse.contents()) {
                s3Client.deleteObject(b -> b.bucket(BUCKET).key(obj.key()));
            }
            s3Client.deleteBucket(b -> b.bucket(BUCKET));
        } catch (Exception e) {
            System.err.println("Failed to cleanup bucket " + BUCKET + ": " + e.getMessage());
        }
        if (s3Client != null) {
            s3Client.close();
        }
    }

    // ==================== 批量上传测试 ====================

    @Test
    @Order(1)
    @DisplayName("批量上传：50个1KB小文件")
    void testBulkUpload_SmallFiles() {
        TestDataGenerator.TestFile[] files = TestDataGenerator.generateBulkTestFiles(BULK_FILE_COUNT, SMALL_FILE_SIZE);

        // Upload all files
        for (TestDataGenerator.TestFile file : files) {
            PutObjectResponse response = s3Client.putObject(
                    b -> b.bucket(BUCKET).key(file.key()).contentType("application/octet-stream"),
                    RequestBody.fromBytes(file.content()));
            assertNotNull(response.eTag(), "ETag should not be null for " + file.key());
        }

        // 验证上传数量
        ListObjectsV2Response listResponse = s3Client.listObjectsV2(b -> b.bucket(BUCKET));
        assertEquals(BULK_FILE_COUNT, listResponse.contents().size(),
                "Should have uploaded " + BULK_FILE_COUNT + " files");

        // 清理
        for (TestDataGenerator.TestFile file : files) {
            s3Client.deleteObject(b -> b.bucket(BUCKET).key(file.key()));
        }
    }

    @Test
    @Order(2)
    @DisplayName("批量上传+完整性验证：逐个验证内容MD5")
    void testBulkUpload_WithContentVerification() {
        // Use fewer files for integrity verification (avoid long test times)
        int verifyCount = 20;
        TestDataGenerator.TestFile[] files = TestDataGenerator.generateBulkTestFiles(verifyCount, SMALL_FILE_SIZE);

        // Upload all files
        for (TestDataGenerator.TestFile file : files) {
            s3Client.putObject(
                    b -> b.bucket(BUCKET).key(file.key()).contentType("application/octet-stream"),
                    RequestBody.fromBytes(file.content()));
        }

        // 逐个下载并验证内容完整性
        for (TestDataGenerator.TestFile file : files) {
            byte[] downloaded = s3Client.getObjectAsBytes(b -> b.bucket(BUCKET).key(file.key())).asByteArray();

            // 验证内容
            DataIntegrityAssert.assertContentEquals(file.content(), downloaded,
                    "Content mismatch for file: " + file.key());

            // 验证 MD5
            String downloadedMd5 = TestDataGenerator.calculateMD5(downloaded);
            assertEquals(file.md5Hex(), downloadedMd5,
                    "MD5 mismatch for file: " + file.key());
        }

        // 清理
        for (TestDataGenerator.TestFile file : files) {
            s3Client.deleteObject(b -> b.bucket(BUCKET).key(file.key()));
        }
    }

    @Test
    @Order(3)
    @DisplayName("批量上传：混合大小文件(100B-100KB)")
    void testBulkUpload_MixedSizes() {
        int mixedCount = 30;
        TestDataGenerator.TestFile[] files = TestDataGenerator.generateMixedSizeTestFiles(
                mixedCount, MIN_MIXED_SIZE, MAX_MIXED_SIZE);

        long totalUploadedBytes = 0;

        // Upload all files
        for (TestDataGenerator.TestFile file : files) {
            s3Client.putObject(
                    b -> b.bucket(BUCKET).key(file.key()).contentType("application/octet-stream"),
                    RequestBody.fromBytes(file.content()));
            totalUploadedBytes += file.size();
        }

        System.out.println("Uploaded " + mixedCount + " files, total size: " + totalUploadedBytes + " bytes");

        // Verify all files can be retrieved correctly
        for (TestDataGenerator.TestFile file : files) {
            HeadObjectResponse headResponse = s3Client.headObject(b -> b.bucket(BUCKET).key(file.key()));
            assertEquals(file.size(), headResponse.contentLength(),
                    "Content-Length mismatch for file: " + file.key());
        }

        // 随机抽样验证内容（验证5个）
        for (int i = 0; i < 5; i++) {
            TestDataGenerator.TestFile file = files[i * (mixedCount / 5)];
            byte[] downloaded = s3Client.getObjectAsBytes(b -> b.bucket(BUCKET).key(file.key())).asByteArray();
            DataIntegrityAssert.assertContentEquals(file.content(), downloaded,
                    "Content mismatch for sampled file: " + file.key());
        }

        // 清理
        for (TestDataGenerator.TestFile file : files) {
            s3Client.deleteObject(b -> b.bucket(BUCKET).key(file.key()));
        }
    }

    @Test
    @Order(4)
    @DisplayName("批量上传后ListObjects验证")
    void testBulkUpload_SequentialThenList() {
        int count = 25;
        TestDataGenerator.TestFile[] files = TestDataGenerator.generateBulkTestFiles(count, SMALL_FILE_SIZE);

        // Upload all files
        for (TestDataGenerator.TestFile file : files) {
            s3Client.putObject(
                    b -> b.bucket(BUCKET).key(file.key()).contentType("application/octet-stream"),
                    RequestBody.fromBytes(file.content()));
        }

        // Verify using ListObjectsV2
        ListObjectsV2Response listResponse = s3Client.listObjectsV2(b -> b.bucket(BUCKET));
        assertEquals(count, listResponse.contents().size(), "ListObjectsV2 should return correct count");

        // Verify all keys exist
        Set<String> uploadedKeys = Arrays.stream(files)
                .map(TestDataGenerator.TestFile::key)
                .collect(Collectors.toSet());
        Set<String> listedKeys = listResponse.contents().stream()
                .map(S3Object::key)
                .collect(Collectors.toSet());
        assertEquals(uploadedKeys, listedKeys, "Listed keys should match uploaded keys");

        // 验证每个对象的 size 正确
        for (S3Object obj : listResponse.contents()) {
            assertEquals(SMALL_FILE_SIZE, obj.size(),
                    "Object size should be " + SMALL_FILE_SIZE + " for " + obj.key());
        }

        // 清理
        for (TestDataGenerator.TestFile file : files) {
            s3Client.deleteObject(b -> b.bucket(BUCKET).key(file.key()));
        }
    }

    @Test
    @Order(5)
    @DisplayName("Batch upload: organize files using prefixes")
    void testBulkUpload_WithPrefix() {
        String prefix = "folder/subfolder/";
        int count = 15;
        TestDataGenerator.TestFile[] files = TestDataGenerator.generateBulkTestFiles(count, SMALL_FILE_SIZE);

        // 上传带前缀的文件
        for (TestDataGenerator.TestFile file : files) {
            String key = prefix + file.key();
            s3Client.putObject(
                    b -> b.bucket(BUCKET).key(key).contentType("application/octet-stream"),
                    RequestBody.fromBytes(file.content()));
        }

        // List with prefix filter
        ListObjectsV2Response listResponse = s3Client.listObjectsV2(
                b -> b.bucket(BUCKET).prefix(prefix));
        assertEquals(count, listResponse.contents().size(),
                "ListObjectsV2 with prefix should return correct count");

        // Verify all keys have correct prefix
        for (S3Object obj : listResponse.contents()) {
            assertTrue(obj.key().startsWith(prefix),
                    "Object key should start with prefix: " + obj.key());
        }

        // 清理
        for (S3Object obj : listResponse.contents()) {
            s3Client.deleteObject(b -> b.bucket(BUCKET).key(obj.key()));
        }
    }

    @Test
    @Order(6)
    @DisplayName("批量删除验证")
    void testBulkUpload_ThenBatchDelete() {
        int count = 20;
        TestDataGenerator.TestFile[] files = TestDataGenerator.generateBulkTestFiles(count, SMALL_FILE_SIZE);

        // Upload all files
        for (TestDataGenerator.TestFile file : files) {
            s3Client.putObject(
                    b -> b.bucket(BUCKET).key(file.key()).contentType("application/octet-stream"),
                    RequestBody.fromBytes(file.content()));
        }

        // 确认上传成功
        ListObjectsV2Response beforeDelete = s3Client.listObjectsV2(b -> b.bucket(BUCKET));
        assertEquals(count, beforeDelete.contents().size(), "Should have uploaded all files");

        // 批量删除
        var toDelete = Arrays.stream(files)
                .map(f -> ObjectIdentifier.builder().key(f.key()).build())
                .toList();

        DeleteObjectsResponse deleteResponse = s3Client.deleteObjects(b -> b
                .bucket(BUCKET)
                .delete(d -> d.objects(toDelete)));

        assertEquals(count, deleteResponse.deleted().size(), "Should have deleted all files");

        // 验证删除成功
        ListObjectsV2Response afterDelete = s3Client.listObjectsV2(b -> b.bucket(BUCKET));
        assertTrue(afterDelete.contents().isEmpty(), "Bucket should be empty after batch delete");
    }
}
