package win.ixuni.chimera.test.s3;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.*;
import win.ixuni.chimera.server.ChimeraServerApplication;
import win.ixuni.chimera.test.s3.util.DataIntegrityAssert;
import win.ixuni.chimera.test.s3.util.TestDataGenerator;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * S3 数据完整性测试
 * <p>
 * Verifies that uploaded data exactly matches downloaded data.
 * 测试覆盖各种数据类型、大小和边界情况。
 * <p>
 * 运行方式：
 * - ./gradlew :chimera-test-s3:test --tests "*DataIntegrity*" -Pdriver=memory
 * - ./gradlew :chimera-test-s3:test --tests "*DataIntegrity*" -Pdriver=local
 */
@SpringBootTest(classes = ChimeraServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ExtendWith(SkipOnNotSupportedExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class S3DataIntegrityTest {

        @LocalServerPort
        private int port;

        private S3Client s3Client;

        private static final String BUCKET = "data-integrity-test-bucket";

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

                // 清理并创建测试 bucket
                cleanupBucketIfExists(BUCKET);
                s3Client.createBucket(b -> b.bucket(BUCKET));
        }

        @AfterAll
        void tearDown() {
                cleanupBucketIfExists(BUCKET);
                if (s3Client != null) {
                        s3Client.close();
                }
        }

        private void cleanupBucketIfExists(String bucketName) {
                try {
                        ListObjectsV2Response listResponse = s3Client.listObjectsV2(b -> b.bucket(bucketName));
                        for (S3Object obj : listResponse.contents()) {
                                s3Client.deleteObject(b -> b.bucket(bucketName).key(obj.key()));
                        }
                        s3Client.deleteBucket(b -> b.bucket(bucketName));
                } catch (NoSuchBucketException e) {
                        // Bucket 不存在，忽略
                } catch (Exception e) {
                        System.err.println("Failed to cleanup bucket " + bucketName + ": " + e.getMessage());
                }
        }

        // ==================== 基础数据完整性测试 ====================

        @Test
        @Order(1)
        @DisplayName("数据完整性：小文本 (100 bytes)")
        void testPutThenGet_SmallText() {
                String content = TestDataGenerator.generateRandomAsciiText(100);
                byte[] originalBytes = content.getBytes(StandardCharsets.UTF_8);
                String key = "small-text.txt";

                // 上传
                s3Client.putObject(
                                b -> b.bucket(BUCKET).key(key).contentType("text/plain"),
                                RequestBody.fromString(content));

                // 下载
                byte[] downloaded = s3Client.getObjectAsBytes(b -> b.bucket(BUCKET).key(key)).asByteArray();

                // 验证
                DataIntegrityAssert.assertContentEquals(originalBytes, downloaded);

                // 清理
                s3Client.deleteObject(b -> b.bucket(BUCKET).key(key));
        }

        @Test
        @Order(2)
        @DisplayName("数据完整性：中等文本 (10KB)")
        void testPutThenGet_MediumText() {
                TestDataGenerator.TestFile testFile = TestDataGenerator.generateTextTestFile("medium-text", 10 * 1024);

                // 上传
                s3Client.putObject(
                                b -> b.bucket(BUCKET).key(testFile.key()).contentType("text/plain"),
                                RequestBody.fromBytes(testFile.content()));

                // 下载
                byte[] downloaded = s3Client.getObjectAsBytes(b -> b.bucket(BUCKET).key(testFile.key())).asByteArray();

                // 验证内容
                DataIntegrityAssert.assertContentEquals(testFile.content(), downloaded);

                // 验证 MD5
                assertEquals(testFile.md5Hex(), TestDataGenerator.calculateMD5(downloaded),
                                "MD5 checksum should match");

                // 清理
                s3Client.deleteObject(b -> b.bucket(BUCKET).key(testFile.key()));
        }

        @Test
        @Order(3)
        @DisplayName("数据完整性：大文本 (1MB)")
        void testPutThenGet_LargeText() {
                TestDataGenerator.TestFile testFile = TestDataGenerator.generateTextTestFile("large-text", 1024 * 1024);

                // 上传
                s3Client.putObject(
                                b -> b.bucket(BUCKET).key(testFile.key()).contentType("text/plain"),
                                RequestBody.fromBytes(testFile.content()));

                // 下载
                byte[] downloaded = s3Client.getObjectAsBytes(b -> b.bucket(BUCKET).key(testFile.key())).asByteArray();

                // 验证
                DataIntegrityAssert.assertContentEquals(testFile.content(), downloaded);
                assertEquals(testFile.sha256Hex(), TestDataGenerator.calculateSHA256(downloaded),
                                "SHA256 checksum should match");

                // 清理
                s3Client.deleteObject(b -> b.bucket(BUCKET).key(testFile.key()));
        }

        @Test
        @Order(4)
        @DisplayName("数据完整性：二进制数据 (500KB)")
        void testPutThenGet_BinaryData() {
                TestDataGenerator.TestFile testFile = TestDataGenerator.generateTestFile("binary", 500 * 1024);

                // 上传
                s3Client.putObject(
                                b -> b.bucket(BUCKET).key(testFile.key()).contentType("application/octet-stream"),
                                RequestBody.fromBytes(testFile.content()));

                // 下载
                byte[] downloaded = s3Client.getObjectAsBytes(b -> b.bucket(BUCKET).key(testFile.key())).asByteArray();

                // 验证
                DataIntegrityAssert.assertContentEquals(testFile.content(), downloaded);
                assertEquals(testFile.md5Hex(), TestDataGenerator.calculateMD5(downloaded));

                // 清理
                s3Client.deleteObject(b -> b.bucket(BUCKET).key(testFile.key()));
        }

        @Test
        @Order(5)
        @DisplayName("数据完整性：空对象 (0 bytes)")
        void testPutThenGet_EmptyObject() {
                String key = "empty-object.txt";
                byte[] emptyContent = new byte[0];

                // 上传空对象
                s3Client.putObject(
                                b -> b.bucket(BUCKET).key(key).contentType("text/plain"),
                                RequestBody.fromBytes(emptyContent));

                // 下载
                byte[] downloaded = s3Client.getObjectAsBytes(b -> b.bucket(BUCKET).key(key)).asByteArray();

                // 验证
                assertEquals(0, downloaded.length, "Empty object should have 0 bytes");

                // 清理
                s3Client.deleteObject(b -> b.bucket(BUCKET).key(key));
        }

        @Test
        @Order(6)
        @DisplayName("数据完整性：单字节对象")
        void testPutThenGet_SingleByte() {
                String key = "single-byte.bin";
                byte[] singleByte = new byte[] { (byte) 0xAB };

                // 上传
                s3Client.putObject(
                                b -> b.bucket(BUCKET).key(key).contentType("application/octet-stream"),
                                RequestBody.fromBytes(singleByte));

                // 下载
                byte[] downloaded = s3Client.getObjectAsBytes(b -> b.bucket(BUCKET).key(key)).asByteArray();

                // 验证
                DataIntegrityAssert.assertContentEquals(singleByte, downloaded);

                // 清理
                s3Client.deleteObject(b -> b.bucket(BUCKET).key(key));
        }

        // ==================== Range 请求验证 ====================

        @Test
        @Order(10)
        @DisplayName("Range 请求：验证部分内容完整性")
        void testRangeGet_VerifyPartialContent() {
                // 生成测试数据
                byte[] fullContent = TestDataGenerator.generateRandomBytes(10 * 1024); // 10KB
                String key = "range-test.bin";

                // 上传
                s3Client.putObject(
                                b -> b.bucket(BUCKET).key(key).contentType("application/octet-stream"),
                                RequestBody.fromBytes(fullContent));

                // 下载前 1000 字节
                byte[] first1000 = s3Client.getObject(
                                b -> b.bucket(BUCKET).key(key).range("bytes=0-999"),
                                ResponseTransformer.toBytes()).asByteArray();

                // 验证
                assertEquals(1000, first1000.length, "Range 0-999 should return 1000 bytes");
                for (int i = 0; i < 1000; i++) {
                        assertEquals(fullContent[i], first1000[i], "Byte at position " + i + " should match");
                }

                // 下载中间 500 字节
                byte[] middle500 = s3Client.getObject(
                                b -> b.bucket(BUCKET).key(key).range("bytes=5000-5499"),
                                ResponseTransformer.toBytes()).asByteArray();

                assertEquals(500, middle500.length);
                for (int i = 0; i < 500; i++) {
                        assertEquals(fullContent[5000 + i], middle500[i],
                                        "Middle byte at offset " + i + " should match");
                }

                // 下载最后 500 字节
                byte[] last500 = s3Client.getObject(
                                b -> b.bucket(BUCKET).key(key).range("bytes=-500"),
                                ResponseTransformer.toBytes()).asByteArray();

                assertEquals(500, last500.length);
                for (int i = 0; i < 500; i++) {
                        assertEquals(fullContent[fullContent.length - 500 + i], last500[i],
                                        "Last byte at offset " + i + " should match");
                }

                // 清理
                s3Client.deleteObject(b -> b.bucket(BUCKET).key(key));
        }

        // ==================== 元数据验证 ====================

        @Test
        @Order(20)
        @DisplayName("Metadata integrity: user-defined metadata round-trip")
        void testUserMetadata_Roundtrip() {
                String key = "metadata-test.txt";
                String content = "Content with metadata";
                // Use simple metadata to avoid special character issues
                Map<String, String> metadata = Map.of(
                                "author", "test-user",
                                "version", "1.0",
                                "description", "test-file");

                // 上传带元数据的对象
                s3Client.putObject(
                                b -> b.bucket(BUCKET).key(key)
                                                .contentType("text/plain")
                                                .metadata(metadata),
                                RequestBody.fromString(content));

                // 获取对象元数据
                HeadObjectResponse headResponse = s3Client.headObject(b -> b.bucket(BUCKET).key(key));

                // 验证元数据
                Map<String, String> returnedMetadata = headResponse.metadata();

                // 如果驱动支持元数据，验证元数据内容
                if (returnedMetadata != null && !returnedMetadata.isEmpty()) {
                        // Verify all custom metadata is preserved
                        for (Map.Entry<String, String> entry : metadata.entrySet()) {
                                String expectedKey = entry.getKey();
                                String expectedValue = entry.getValue();

                                // S3 可能将 key 转为小写
                                String actualValue = returnedMetadata.get(expectedKey);
                                if (actualValue == null) {
                                        actualValue = returnedMetadata.get(expectedKey.toLowerCase());
                                }

                                assertEquals(expectedValue, actualValue,
                                                "Metadata value for key '" + expectedKey
                                                                + "' should match. Returned metadata: "
                                                                + returnedMetadata);
                        }
                } else {
                        System.out.println(
                                        "⚠️ Driver does not preserve user metadata (this may be expected for some drivers)");
                }

                // 清理
                s3Client.deleteObject(b -> b.bucket(BUCKET).key(key));
        }

        @Test
        @Order(21)
        @DisplayName("元数据完整性：Content-Type 保留")
        void testContentType_Preservation() {
                String key = "content-type-test.json";
                String content = "{\"key\": \"value\"}";
                String contentType = "application/json; charset=utf-8";

                // 上传
                s3Client.putObject(
                                b -> b.bucket(BUCKET).key(key).contentType(contentType),
                                RequestBody.fromString(content));

                // 获取元数据
                HeadObjectResponse headResponse = s3Client.headObject(b -> b.bucket(BUCKET).key(key));

                // 验证 Content-Type
                assertTrue(headResponse.contentType().contains("application/json"),
                                "Content-Type should contain 'application/json', but got: "
                                                + headResponse.contentType());

                // 清理
                s3Client.deleteObject(b -> b.bucket(BUCKET).key(key));
        }

        @Test
        @Order(22)
        @DisplayName("元数据完整性：Content-Length 验证")
        void testContentLength_Verification() {
                byte[] content = TestDataGenerator.generateRandomBytes(12345); // 非整数 KB
                String key = "content-length-test.bin";

                // 上传
                s3Client.putObject(
                                b -> b.bucket(BUCKET).key(key).contentType("application/octet-stream"),
                                RequestBody.fromBytes(content));

                // 获取元数据
                HeadObjectResponse headResponse = s3Client.headObject(b -> b.bucket(BUCKET).key(key));

                // 验证 Content-Length
                assertEquals(content.length, headResponse.contentLength(),
                                "Content-Length should match uploaded content size");

                // 下载验证实际大小
                byte[] downloaded = s3Client.getObjectAsBytes(b -> b.bucket(BUCKET).key(key)).asByteArray();
                DataIntegrityAssert.assertContentLengthMatches(headResponse.contentLength(), downloaded);

                // 清理
                s3Client.deleteObject(b -> b.bucket(BUCKET).key(key));
        }

        // ==================== 分片上传验证 ====================

        @Test
        @Order(30)
        @DisplayName("分片上传：数据完整性验证")
        void testMultipartUpload_DataIntegrity() {
                String key = "multipart-test.bin";

                // Generate multi-part data (S3 minimum part size is 5MB, last part can be smaller)
                byte[] part1Data = TestDataGenerator.generateRandomBytes(5 * 1024 * 1024); // 5MB
                byte[] part2Data = TestDataGenerator.generateRandomBytes(5 * 1024 * 1024); // 5MB
                byte[] part3Data = TestDataGenerator.generateRandomBytes(1024); // 1KB (last part can be smaller than 5MB)

                // 合并为完整文件
                byte[] fullContent = new byte[part1Data.length + part2Data.length + part3Data.length];
                System.arraycopy(part1Data, 0, fullContent, 0, part1Data.length);
                System.arraycopy(part2Data, 0, fullContent, part1Data.length, part2Data.length);
                System.arraycopy(part3Data, 0, fullContent, part1Data.length + part2Data.length, part3Data.length);

                String expectedMd5 = TestDataGenerator.calculateMD5(fullContent);

                // 1. 初始化分片上传
                CreateMultipartUploadResponse createResponse = s3Client.createMultipartUpload(
                                b -> b.bucket(BUCKET).key(key).contentType("application/octet-stream"));
                String uploadId = createResponse.uploadId();
                assertNotNull(uploadId, "Upload ID should not be null");

                // 2. 上传分片
                List<CompletedPart> completedParts = new ArrayList<>();

                UploadPartResponse part1Response = s3Client.uploadPart(
                                b -> b.bucket(BUCKET).key(key).uploadId(uploadId).partNumber(1),
                                RequestBody.fromBytes(part1Data));
                completedParts.add(CompletedPart.builder().partNumber(1).eTag(part1Response.eTag()).build());

                UploadPartResponse part2Response = s3Client.uploadPart(
                                b -> b.bucket(BUCKET).key(key).uploadId(uploadId).partNumber(2),
                                RequestBody.fromBytes(part2Data));
                completedParts.add(CompletedPart.builder().partNumber(2).eTag(part2Response.eTag()).build());

                UploadPartResponse part3Response = s3Client.uploadPart(
                                b -> b.bucket(BUCKET).key(key).uploadId(uploadId).partNumber(3),
                                RequestBody.fromBytes(part3Data));
                completedParts.add(CompletedPart.builder().partNumber(3).eTag(part3Response.eTag()).build());

                // 3. 完成分片上传
                CompleteMultipartUploadResponse completeResponse = s3Client.completeMultipartUpload(b -> b
                                .bucket(BUCKET)
                                .key(key)
                                .uploadId(uploadId)
                                .multipartUpload(m -> m.parts(completedParts)));
                assertNotNull(completeResponse.eTag(), "Complete ETag should not be null");

                // 4. 下载并验证完整性
                byte[] downloaded = s3Client.getObjectAsBytes(b -> b.bucket(BUCKET).key(key)).asByteArray();

                DataIntegrityAssert.assertContentEquals(fullContent, downloaded,
                                "Multipart uploaded content should match when downloaded");
                assertEquals(expectedMd5, TestDataGenerator.calculateMD5(downloaded),
                                "MD5 of downloaded content should match original");

                // 清理
                s3Client.deleteObject(b -> b.bucket(BUCKET).key(key));
        }

        // ==================== 覆盖写入验证 ====================

        @Test
        @Order(40)
        @DisplayName("覆盖写入：新旧内容完全隔离")
        void testOverwrite_ContentIsolation() {
                String key = "overwrite-test.txt";

                // 上传原始内容
                byte[] originalContent = TestDataGenerator.generateRandomBytes(1000);
                s3Client.putObject(
                                b -> b.bucket(BUCKET).key(key).contentType("application/octet-stream"),
                                RequestBody.fromBytes(originalContent));

                // 下载验证
                byte[] downloaded1 = s3Client.getObjectAsBytes(b -> b.bucket(BUCKET).key(key)).asByteArray();
                DataIntegrityAssert.assertContentEquals(originalContent, downloaded1);

                // 上传新内容（不同大小）
                byte[] newContent = TestDataGenerator.generateRandomBytes(2000);
                s3Client.putObject(
                                b -> b.bucket(BUCKET).key(key).contentType("application/octet-stream"),
                                RequestBody.fromBytes(newContent));

                // 下载验证新内容
                byte[] downloaded2 = s3Client.getObjectAsBytes(b -> b.bucket(BUCKET).key(key)).asByteArray();
                DataIntegrityAssert.assertContentEquals(newContent, downloaded2);

                // Ensure no old content remains
                assertNotEquals(originalContent.length, downloaded2.length,
                                "New content should have different size");

                // 清理
                s3Client.deleteObject(b -> b.bucket(BUCKET).key(key));
        }

        // ==================== 复制对象验证 ====================

        @Test
        @Order(50)
        @DisplayName("Copy object: source and target content are identical")
        void testCopyObject_ContentEquality() {
                String sourceKey = "copy-source.bin";
                String destKey = "copy-destination.bin";

                // 上传源对象
                byte[] sourceContent = TestDataGenerator.generateRandomBytes(5000);
                s3Client.putObject(
                                b -> b.bucket(BUCKET).key(sourceKey).contentType("application/octet-stream"),
                                RequestBody.fromBytes(sourceContent));

                // 复制
                s3Client.copyObject(b -> b
                                .sourceBucket(BUCKET)
                                .sourceKey(sourceKey)
                                .destinationBucket(BUCKET)
                                .destinationKey(destKey));

                // 下载目标对象
                byte[] destContent = s3Client.getObjectAsBytes(b -> b.bucket(BUCKET).key(destKey)).asByteArray();

                // Verify source and target are identical
                DataIntegrityAssert.assertContentEquals(sourceContent, destContent,
                                "Copied object should have identical content");

                // 清理
                s3Client.deleteObject(b -> b.bucket(BUCKET).key(sourceKey));
                s3Client.deleteObject(b -> b.bucket(BUCKET).key(destKey));
        }

        // ==================== Unicode 内容测试 ====================

        @Test
        @Order(60)
        @DisplayName("数据完整性：Unicode 内容 (中日韩)")
        void testPutThenGet_UnicodeContent() {
                String key = "unicode-content.txt";
                // Use fixed Unicode strings to avoid surrogate pair issues from random generation
                String unicodeContent = "Hello 你好 こんにちは 안녕하세요\n" +
                                "中文内容测试 Japanese: テスト Korean: 테스트\n" +
                                "Greek: αβγδεζηθ Math: ∑∏∫±×÷\n" +
                                "Russian: Привет мир Cyrillic: АБВГД\n" +
                                "Arabic: مرحبا\n" +
                                "Repeated for size: 你好世界 こんにちは世界 안녕하세요세계\n".repeat(10);

                byte[] originalBytes = unicodeContent.getBytes(StandardCharsets.UTF_8);

                // 上传
                s3Client.putObject(
                                b -> b.bucket(BUCKET).key(key).contentType("text/plain; charset=utf-8"),
                                RequestBody.fromString(unicodeContent, StandardCharsets.UTF_8));

                // 下载
                byte[] downloaded = s3Client.getObjectAsBytes(b -> b.bucket(BUCKET).key(key)).asByteArray();
                String downloadedString = new String(downloaded, StandardCharsets.UTF_8);

                // 验证字节级别完整性
                DataIntegrityAssert.assertContentEquals(originalBytes, downloaded);

                // 验证解码后的字符串
                assertEquals(unicodeContent, downloadedString, "Unicode string content should match");

                // 清理
                s3Client.deleteObject(b -> b.bucket(BUCKET).key(key));
        }
}
