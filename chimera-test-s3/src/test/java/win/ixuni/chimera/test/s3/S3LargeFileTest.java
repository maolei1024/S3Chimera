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
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * S3 大文件上传测试
 * <p>
 * 测试大文件的上传、分片上传和 Range 请求功能。
 * 验证大文件完整性（MD5/SHA256）和部分内容下载。
 * <p>
 * 运行方式：
 * - ./gradlew :chimera-test-s3:test --tests "*LargeFile*" -Pdriver=memory
 * - ./gradlew :chimera-test-s3:test --tests "*LargeFile*" -Pdriver=local
 */
@SpringBootTest(classes = ChimeraServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ExtendWith(SkipOnNotSupportedExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Timeout(value = 180, unit = TimeUnit.SECONDS) // 大文件测试允许更长时间
public class S3LargeFileTest {

        @LocalServerPort
        private int port;

        private S3Client s3Client;

        // Use unique bucket name to avoid conflicts with other tests
        private final String BUCKET = "large-file-test-" + UUID.randomUUID().toString().substring(0, 8);

        // CI-friendly size limits
        private static final int LARGE_FILE_SIZE = 10 * 1024 * 1024; // 10MB
        private static final int MULTIPART_PART_SIZE = 5 * 1024 * 1024; // 5MB (S3 最小分片大小)

        @BeforeAll
        void setup() {
                S3Configuration s3Config = S3Configuration.builder()
                                .pathStyleAccessEnabled(true)
                                .checksumValidationEnabled(true)
                                .chunkedEncodingEnabled(true)
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

        // ==================== 大文件直接上传测试 ====================

        @Test
        @Order(1)
        @DisplayName("大文件直传：10MB文件+完整性验证")
        void testLargeFile_10MB_DirectUpload() {
                byte[] largeContent = TestDataGenerator.generateRandomBytes(LARGE_FILE_SIZE);
                String key = "large-10mb-" + UUID.randomUUID().toString().substring(0, 8) + ".bin";
                String originalMd5 = TestDataGenerator.calculateMD5(largeContent);
                String originalSha256 = TestDataGenerator.calculateSHA256(largeContent);

                // 上传
                long startTime = System.currentTimeMillis();
                PutObjectResponse response = s3Client.putObject(
                                b -> b.bucket(BUCKET).key(key).contentType("application/octet-stream"),
                                RequestBody.fromBytes(largeContent));
                long uploadTime = System.currentTimeMillis() - startTime;

                assertNotNull(response.eTag(), "ETag should not be null");
                System.out.println("Uploaded 10MB in " + uploadTime + "ms");

                // 验证 HeadObject
                HeadObjectResponse headResponse = s3Client.headObject(b -> b.bucket(BUCKET).key(key));
                assertEquals(LARGE_FILE_SIZE, headResponse.contentLength(),
                                "Content-Length should match");

                // 下载并验证完整性
                startTime = System.currentTimeMillis();
                byte[] downloaded = s3Client.getObjectAsBytes(b -> b.bucket(BUCKET).key(key)).asByteArray();
                long downloadTime = System.currentTimeMillis() - startTime;

                System.out.println("Downloaded 10MB in " + downloadTime + "ms");

                // 验证大小
                assertEquals(largeContent.length, downloaded.length, "Downloaded size should match");

                // 验证 MD5
                String downloadedMd5 = TestDataGenerator.calculateMD5(downloaded);
                assertEquals(originalMd5, downloadedMd5, "MD5 should match");

                // 验证 SHA256
                String downloadedSha256 = TestDataGenerator.calculateSHA256(downloaded);
                assertEquals(originalSha256, downloadedSha256, "SHA256 should match");

                // 清理
                s3Client.deleteObject(b -> b.bucket(BUCKET).key(key));
        }

        // ==================== 分片上传测试 ====================

        @Test
        @Order(2)
        @DisplayName("分片上传：20MB (4×5MB 分片) + 完整性验证")
        void testLargeFile_Multipart_20MB() {
                String key = "multipart-20mb-" + UUID.randomUUID().toString().substring(0, 8) + ".bin";

                // 生成 4 个 5MB 分片
                int partCount = 4;
                byte[][] parts = new byte[partCount][];
                for (int i = 0; i < partCount; i++) {
                        parts[i] = TestDataGenerator.generateRandomBytes(MULTIPART_PART_SIZE);
                }

                // 合并为完整内容
                byte[] fullContent = new byte[partCount * MULTIPART_PART_SIZE];
                int offset = 0;
                for (byte[] part : parts) {
                        System.arraycopy(part, 0, fullContent, offset, part.length);
                        offset += part.length;
                }
                String expectedMd5 = TestDataGenerator.calculateMD5(fullContent);

                // 1. 初始化分片上传
                CreateMultipartUploadResponse createResponse = s3Client.createMultipartUpload(
                                b -> b.bucket(BUCKET).key(key).contentType("application/octet-stream"));
                String uploadId = createResponse.uploadId();
                assertNotNull(uploadId, "Upload ID should not be null");

                // 2. 上传分片
                List<CompletedPart> completedParts = new ArrayList<>();
                for (int i = 0; i < partCount; i++) {
                        final int partNumber = i + 1;
                        final byte[] partData = parts[i];

                        UploadPartResponse partResponse = s3Client.uploadPart(
                                        b -> b.bucket(BUCKET).key(key).uploadId(uploadId).partNumber(partNumber),
                                        RequestBody.fromBytes(partData));
                        completedParts.add(CompletedPart.builder()
                                        .partNumber(partNumber)
                                        .eTag(partResponse.eTag())
                                        .build());
                }

                // 3. 完成分片上传
                CompleteMultipartUploadResponse completeResponse = s3Client.completeMultipartUpload(b -> b
                                .bucket(BUCKET)
                                .key(key)
                                .uploadId(uploadId)
                                .multipartUpload(m -> m.parts(completedParts)));
                assertNotNull(completeResponse.eTag(), "Complete ETag should not be null");

                // 4. 验证内容大小
                HeadObjectResponse headResponse = s3Client.headObject(b -> b.bucket(BUCKET).key(key));
                assertEquals(fullContent.length, headResponse.contentLength(),
                                "Content-Length should match total parts size");

                // 5. 下载并验证完整性
                byte[] downloaded = s3Client.getObjectAsBytes(b -> b.bucket(BUCKET).key(key)).asByteArray();
                DataIntegrityAssert.assertContentEquals(fullContent, downloaded,
                                "Multipart upload content should match");
                assertEquals(expectedMd5, TestDataGenerator.calculateMD5(downloaded),
                                "MD5 should match");

                // 清理
                s3Client.deleteObject(b -> b.bucket(BUCKET).key(key));
        }

        // ==================== Range 请求测试 ====================

        @Test
        @Order(3)
        @DisplayName("Range请求：大文件多点验证")
        void testLargeFile_RangeRequests() {
                byte[] largeContent = TestDataGenerator.generateRandomBytes(LARGE_FILE_SIZE);
                String key = "range-test-" + UUID.randomUUID().toString().substring(0, 8) + ".bin";

                // 上传
                s3Client.putObject(
                                b -> b.bucket(BUCKET).key(key).contentType("application/octet-stream"),
                                RequestBody.fromBytes(largeContent));

                // 测试多个 Range 请求

                // 1. 前 1MB
                byte[] first1MB = s3Client.getObject(
                                b -> b.bucket(BUCKET).key(key).range("bytes=0-1048575"),
                                ResponseTransformer.toBytes()).asByteArray();
                assertEquals(1024 * 1024, first1MB.length, "First 1MB should be 1048576 bytes");
                for (int i = 0; i < first1MB.length; i++) {
                        assertEquals(largeContent[i], first1MB[i], "Byte at position " + i + " should match");
                }

                // 2. 中间 500KB (从 5MB 开始)
                int midStart = 5 * 1024 * 1024;
                int midEnd = midStart + 512 * 1024 - 1;
                byte[] middle500KB = s3Client.getObject(
                                b -> b.bucket(BUCKET).key(key).range("bytes=" + midStart + "-" + midEnd),
                                ResponseTransformer.toBytes()).asByteArray();
                assertEquals(512 * 1024, middle500KB.length, "Middle 500KB should be 524288 bytes");
                for (int i = 0; i < middle500KB.length; i++) {
                        assertEquals(largeContent[midStart + i], middle500KB[i],
                                        "Middle byte at offset " + i + " should match");
                }

                // 3. 最后 100KB
                byte[] last100KB = s3Client.getObject(
                                b -> b.bucket(BUCKET).key(key).range("bytes=-102400"),
                                ResponseTransformer.toBytes()).asByteArray();
                assertEquals(100 * 1024, last100KB.length, "Last 100KB should be 102400 bytes");
                int lastStart = largeContent.length - last100KB.length;
                for (int i = 0; i < last100KB.length; i++) {
                        assertEquals(largeContent[lastStart + i], last100KB[i],
                                        "Last byte at offset " + i + " should match");
                }

                // 清理
                s3Client.deleteObject(b -> b.bucket(BUCKET).key(key));
        }

        @Test
        @Order(4)
        @DisplayName("Range请求：边界条件测试")
        void testLargeFile_RangeBoundary() {
                int size = 1024 * 100; // 100KB
                byte[] content = TestDataGenerator.generateRandomBytes(size);
                String key = "range-boundary-" + UUID.randomUUID().toString().substring(0, 8) + ".bin";

                s3Client.putObject(
                                b -> b.bucket(BUCKET).key(key).contentType("application/octet-stream"),
                                RequestBody.fromBytes(content));

                // 1. Get the first byte
                byte[] firstByte = s3Client.getObject(
                                b -> b.bucket(BUCKET).key(key).range("bytes=0-0"),
                                ResponseTransformer.toBytes()).asByteArray();
                assertEquals(1, firstByte.length, "Should get exactly 1 byte");
                assertEquals(content[0], firstByte[0], "First byte should match");

                // 2. Get the last byte
                byte[] lastByte = s3Client.getObject(
                                b -> b.bucket(BUCKET).key(key).range("bytes=-1"),
                                ResponseTransformer.toBytes()).asByteArray();
                assertEquals(1, lastByte.length, "Should get exactly 1 byte");
                assertEquals(content[content.length - 1], lastByte[0], "Last byte should match");

                // 3. 获取恰好 1KB
                byte[] exactly1KB = s3Client.getObject(
                                b -> b.bucket(BUCKET).key(key).range("bytes=0-1023"),
                                ResponseTransformer.toBytes()).asByteArray();
                assertEquals(1024, exactly1KB.length, "Should get exactly 1024 bytes");
                for (int i = 0; i < 1024; i++) {
                        assertEquals(content[i], exactly1KB[i], "Byte at " + i + " should match");
                }

                // 清理
                s3Client.deleteObject(b -> b.bucket(BUCKET).key(key));
        }

        // ==================== 覆盖写入测试 ====================

        @Test
        @Order(5)
        @DisplayName("大文件覆盖：新旧内容隔离验证")
        void testLargeFile_Overwrite() {
                String key = "overwrite-test-" + UUID.randomUUID().toString().substring(0, 8) + ".bin";

                // 上传原始大文件 (5MB)
                byte[] originalContent = TestDataGenerator.generateRandomBytes(5 * 1024 * 1024);
                String originalMd5 = TestDataGenerator.calculateMD5(originalContent);
                s3Client.putObject(
                                b -> b.bucket(BUCKET).key(key).contentType("application/octet-stream"),
                                RequestBody.fromBytes(originalContent));

                // 验证原始上传
                byte[] downloaded1 = s3Client.getObjectAsBytes(b -> b.bucket(BUCKET).key(key)).asByteArray();
                assertEquals(originalMd5, TestDataGenerator.calculateMD5(downloaded1),
                                "Original content MD5 should match");

                // 覆盖写入不同大小的新文件 (3MB)
                byte[] newContent = TestDataGenerator.generateRandomBytes(3 * 1024 * 1024);
                String newMd5 = TestDataGenerator.calculateMD5(newContent);
                s3Client.putObject(
                                b -> b.bucket(BUCKET).key(key).contentType("application/octet-stream"),
                                RequestBody.fromBytes(newContent));

                // 验证覆盖后内容
                byte[] downloaded2 = s3Client.getObjectAsBytes(b -> b.bucket(BUCKET).key(key)).asByteArray();
                assertEquals(newContent.length, downloaded2.length, "New content size should match");
                assertEquals(newMd5, TestDataGenerator.calculateMD5(downloaded2),
                                "New content MD5 should match");

                // Ensure no old content remains
                assertNotEquals(originalContent.length, downloaded2.length,
                                "New content should have different size than original");

                // 清理
                s3Client.deleteObject(b -> b.bucket(BUCKET).key(key));
        }
}
