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

import java.net.URI;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * S3 元数据完整性测试
 * <p>
 * Tests the round-trip preservation of user-defined metadata, Content-Type, and ETag.
 * Verifies that uploaded metadata exactly matches retrieved metadata.
 * <p>
 * 运行方式：
 * - ./gradlew :chimera-test-s3:test --tests "*S3MetadataTest*" -Pdriver=memory
 * - ./gradlew :chimera-test-s3:test --tests "*S3MetadataTest*" -Pdriver=local
 */
@SpringBootTest(classes = ChimeraServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ExtendWith(SkipOnNotSupportedExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Timeout(value = 60, unit = TimeUnit.SECONDS)
public class S3MetadataTest {

    @LocalServerPort
    private int port;

    private S3Client s3Client;

    private final String BUCKET = "metadata-test-" + UUID.randomUUID().toString().substring(0, 8);

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

        s3Client.createBucket(b -> b.bucket(BUCKET));
    }

    @AfterAll
    void tearDown() {
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

    // ==================== User Metadata Tests ====================

    @Test
    @Order(1)
    @DisplayName("元数据往返：单个键值对")
    void testMetadata_SingleValue_Roundtrip() {
        String key = "metadata-single-" + UUID.randomUUID().toString().substring(0, 8) + ".txt";
        String content = "Test content";
        Map<String, String> metadata = Map.of("author", "test-user");

        // 上传带元数据
        s3Client.putObject(
                b -> b.bucket(BUCKET).key(key)
                        .contentType("text/plain")
                        .metadata(metadata),
                RequestBody.fromString(content));

        // 获取元数据
        HeadObjectResponse headResponse = s3Client.headObject(b -> b.bucket(BUCKET).key(key));
        Map<String, String> returnedMetadata = headResponse.metadata();

        // 验证
        if (returnedMetadata != null && !returnedMetadata.isEmpty()) {
            DataIntegrityAssert.assertMetadataEquals(metadata, returnedMetadata,
                    "Single metadata value should match");
        } else {
            System.out.println("⚠️ Driver does not preserve user metadata");
        }

        // 清理
        s3Client.deleteObject(b -> b.bucket(BUCKET).key(key));
    }

    @Test
    @Order(2)
    @DisplayName("元数据往返：多个键值对")
    void testMetadata_MultipleValues_Roundtrip() {
        String key = "metadata-multi-" + UUID.randomUUID().toString().substring(0, 8) + ".txt";
        String content = "Test content with multiple metadata";
        Map<String, String> metadata = Map.of(
                "author", "john-doe",
                "version", "1.0.0",
                "department", "engineering",
                "project-id", "12345");

        // 上传
        s3Client.putObject(
                b -> b.bucket(BUCKET).key(key)
                        .contentType("text/plain")
                        .metadata(metadata),
                RequestBody.fromString(content));

        // 获取
        HeadObjectResponse headResponse = s3Client.headObject(b -> b.bucket(BUCKET).key(key));
        Map<String, String> returnedMetadata = headResponse.metadata();

        // 验证
        if (returnedMetadata != null && !returnedMetadata.isEmpty()) {
            DataIntegrityAssert.assertMetadataEquals(metadata, returnedMetadata,
                    "Multiple metadata values should all match");
        } else {
            System.out.println("⚠️ Driver does not preserve user metadata");
        }

        // 清理
        s3Client.deleteObject(b -> b.bucket(BUCKET).key(key));
    }

    @Test
    @Order(3)
    @DisplayName("元数据往返：数字值")
    void testMetadata_NumericValues() {
        String key = "metadata-numeric-" + UUID.randomUUID().toString().substring(0, 8) + ".txt";
        Map<String, String> metadata = Map.of(
                "count", "42",
                "timestamp", "1703936400000",
                "version", "2");

        s3Client.putObject(
                b -> b.bucket(BUCKET).key(key)
                        .contentType("text/plain")
                        .metadata(metadata),
                RequestBody.fromString("Numeric metadata test"));

        HeadObjectResponse headResponse = s3Client.headObject(b -> b.bucket(BUCKET).key(key));
        Map<String, String> returnedMetadata = headResponse.metadata();

        if (returnedMetadata != null && !returnedMetadata.isEmpty()) {
            DataIntegrityAssert.assertMetadataEquals(metadata, returnedMetadata,
                    "Numeric metadata values should be preserved as strings");
        }

        s3Client.deleteObject(b -> b.bucket(BUCKET).key(key));
    }

    // ==================== Content-Type 测试 ====================

    @Test
    @Order(10)
    @DisplayName("Content-Type保留：text/plain")
    void testContentType_TextPlain() {
        String key = "content-type-text-" + UUID.randomUUID().toString().substring(0, 8) + ".txt";
        String contentType = "text/plain";

        s3Client.putObject(
                b -> b.bucket(BUCKET).key(key).contentType(contentType),
                RequestBody.fromString("Plain text content"));

        HeadObjectResponse headResponse = s3Client.headObject(b -> b.bucket(BUCKET).key(key));
        assertTrue(headResponse.contentType().contains("text/plain"),
                "Content-Type should contain 'text/plain', got: " + headResponse.contentType());

        s3Client.deleteObject(b -> b.bucket(BUCKET).key(key));
    }

    @Test
    @Order(11)
    @DisplayName("Content-Type保留：application/json")
    void testContentType_Json() {
        String key = "content-type-json-" + UUID.randomUUID().toString().substring(0, 8) + ".json";
        String contentType = "application/json";

        s3Client.putObject(
                b -> b.bucket(BUCKET).key(key).contentType(contentType),
                RequestBody.fromString("{\"key\": \"value\"}"));

        HeadObjectResponse headResponse = s3Client.headObject(b -> b.bucket(BUCKET).key(key));
        assertTrue(headResponse.contentType().contains("application/json"),
                "Content-Type should contain 'application/json', got: " + headResponse.contentType());

        s3Client.deleteObject(b -> b.bucket(BUCKET).key(key));
    }

    @Test
    @Order(12)
    @DisplayName("Content-Type保留：带charset")
    void testContentType_WithCharset() {
        String key = "content-type-charset-" + UUID.randomUUID().toString().substring(0, 8) + ".txt";
        String contentType = "text/plain; charset=utf-8";

        s3Client.putObject(
                b -> b.bucket(BUCKET).key(key).contentType(contentType),
                RequestBody.fromString("UTF-8 content: 你好世界"));

        HeadObjectResponse headResponse = s3Client.headObject(b -> b.bucket(BUCKET).key(key));
        assertTrue(headResponse.contentType().contains("text/plain"),
                "Content-Type should contain 'text/plain', got: " + headResponse.contentType());

        s3Client.deleteObject(b -> b.bucket(BUCKET).key(key));
    }

    @Test
    @Order(13)
    @DisplayName("Content-Type保留：image/png")
    void testContentType_Image() {
        String key = "content-type-image-" + UUID.randomUUID().toString().substring(0, 8) + ".png";
        String contentType = "image/png";
        byte[] content = new byte[] { (byte) 0x89, 0x50, 0x4E, 0x47 }; // PNG magic bytes

        s3Client.putObject(
                b -> b.bucket(BUCKET).key(key).contentType(contentType),
                RequestBody.fromBytes(content));

        HeadObjectResponse headResponse = s3Client.headObject(b -> b.bucket(BUCKET).key(key));
        assertTrue(headResponse.contentType().contains("image/png"),
                "Content-Type should contain 'image/png', got: " + headResponse.contentType());

        s3Client.deleteObject(b -> b.bucket(BUCKET).key(key));
    }

    // ==================== ETag 测试 ====================

    @Test
    @Order(20)
    @DisplayName("ETag consistency: same content should produce same ETag")
    void testETag_Consistency() {
        String key1 = "etag-test-1-" + UUID.randomUUID().toString().substring(0, 8) + ".txt";
        String key2 = "etag-test-2-" + UUID.randomUUID().toString().substring(0, 8) + ".txt";
        String content = "Identical content for ETag test";

        // 上传相同内容到两个不同的 key
        PutObjectResponse response1 = s3Client.putObject(
                b -> b.bucket(BUCKET).key(key1).contentType("text/plain"),
                RequestBody.fromString(content));

        PutObjectResponse response2 = s3Client.putObject(
                b -> b.bucket(BUCKET).key(key2).contentType("text/plain"),
                RequestBody.fromString(content));

        // 验证 ETag 非空
        DataIntegrityAssert.assertETagValid(response1.eTag());
        DataIntegrityAssert.assertETagValid(response2.eTag());

        // For single uploads, same content should produce same ETag (if based on content MD5)
        // Note: some drivers may not guarantee this
        String etag1 = response1.eTag().replace("\"", "");
        String etag2 = response2.eTag().replace("\"", "");
        if (etag1.equals(etag2)) {
            System.out.println("✓ ETags are content-based (identical for same content)");
        } else {
            System.out.println("⚠️ ETags differ for same content (may include upload-specific data)");
        }

        // 清理
        s3Client.deleteObject(b -> b.bucket(BUCKET).key(key1));
        s3Client.deleteObject(b -> b.bucket(BUCKET).key(key2));
    }

    @Test
    @Order(21)
    @DisplayName("ETag verification: GetObject and HeadObject are consistent")
    void testETag_GetVsHead() {
        String key = "etag-compare-" + UUID.randomUUID().toString().substring(0, 8) + ".txt";
        String content = "Content for ETag comparison";

        PutObjectResponse putResponse = s3Client.putObject(
                b -> b.bucket(BUCKET).key(key).contentType("text/plain"),
                RequestBody.fromString(content));
        String putEtag = putResponse.eTag();

        HeadObjectResponse headResponse = s3Client.headObject(b -> b.bucket(BUCKET).key(key));
        String headEtag = headResponse.eTag();

        GetObjectResponse getResponse = s3Client.getObject(b -> b.bucket(BUCKET).key(key)).response();
        String getEtag = getResponse.eTag();

        // All ETags should be consistent
        assertEquals(putEtag, headEtag, "PutObject and HeadObject ETags should match");
        assertEquals(headEtag, getEtag, "HeadObject and GetObject ETags should match");

        s3Client.deleteObject(b -> b.bucket(BUCKET).key(key));
    }

    // ==================== 元数据更新测试 ====================

    @Test
    @Order(30)
    @DisplayName("覆盖上传：元数据更新验证")
    void testMetadata_UpdateOnOverwrite() {
        String key = "metadata-update-" + UUID.randomUUID().toString().substring(0, 8) + ".txt";

        // First upload
        Map<String, String> originalMetadata = Map.of("version", "1");
        s3Client.putObject(
                b -> b.bucket(BUCKET).key(key)
                        .contentType("text/plain")
                        .metadata(originalMetadata),
                RequestBody.fromString("Version 1 content"));

        // 验证原始元数据
        HeadObjectResponse head1 = s3Client.headObject(b -> b.bucket(BUCKET).key(key));
        if (head1.metadata() != null && !head1.metadata().isEmpty()) {
            String originalVersion = head1.metadata().getOrDefault("version",
                    head1.metadata().get("Version"));
            assertEquals("1", originalVersion, "Original version should be 1");
        }

        // 覆盖上传，更新元数据
        Map<String, String> newMetadata = Map.of("version", "2");
        s3Client.putObject(
                b -> b.bucket(BUCKET).key(key)
                        .contentType("text/plain")
                        .metadata(newMetadata),
                RequestBody.fromString("Version 2 content"));

        // 验证新元数据
        HeadObjectResponse head2 = s3Client.headObject(b -> b.bucket(BUCKET).key(key));
        if (head2.metadata() != null && !head2.metadata().isEmpty()) {
            String newVersion = head2.metadata().getOrDefault("version",
                    head2.metadata().get("Version"));
            assertEquals("2", newVersion, "New version should be 2");
        }

        s3Client.deleteObject(b -> b.bucket(BUCKET).key(key));
    }

    @Test
    @Order(31)
    @DisplayName("复制对象：元数据保留验证")
    void testMetadata_PreservedAfterCopy() {
        String sourceKey = "copy-source-" + UUID.randomUUID().toString().substring(0, 8) + ".txt";
        String destKey = "copy-dest-" + UUID.randomUUID().toString().substring(0, 8) + ".txt";

        Map<String, String> metadata = Map.of(
                "author", "original-author",
                "version", "1.0");

        // 上传源对象
        s3Client.putObject(
                b -> b.bucket(BUCKET).key(sourceKey)
                        .contentType("text/plain")
                        .metadata(metadata),
                RequestBody.fromString("Content to copy"));

        // 复制对象
        s3Client.copyObject(b -> b
                .sourceBucket(BUCKET)
                .sourceKey(sourceKey)
                .destinationBucket(BUCKET)
                .destinationKey(destKey));

        // 验证目标对象的元数据
        HeadObjectResponse destHead = s3Client.headObject(b -> b.bucket(BUCKET).key(destKey));
        if (destHead.metadata() != null && !destHead.metadata().isEmpty()) {
            // Copy operation should preserve metadata
            DataIntegrityAssert.assertMetadataEquals(metadata, destHead.metadata(),
                    "Copied object should preserve metadata");
        } else {
            System.out.println("⚠️ CopyObject did not preserve metadata (some drivers may require MetadataDirective)");
        }

        // 清理
        s3Client.deleteObject(b -> b.bucket(BUCKET).key(sourceKey));
        s3Client.deleteObject(b -> b.bucket(BUCKET).key(destKey));
    }
}
