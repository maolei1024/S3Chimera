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

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * S3 兼容性测试
 * <p>
 * Uses AWS S3 SDK as client to verify S3Chimera API compatibility.
 * 每个测试场景都包含成功和失败两种情况的验证。
 * <p>
 * 运行方式:
 * - ./gradlew :chimera-test-s3:test -Pdriver=memory
 * - ./gradlew :chimera-test-s3:test -Pdriver=local
 * - ./gradlew :chimera-test-s3:test -Pdriver=mysql
 * - ./gradlew :chimera-test-s3:test -Pdriver=s3
 * - ./gradlew :chimera-test-s3:test -Pdriver=webdav
 */
@SpringBootTest(classes = ChimeraServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ExtendWith(SkipOnNotSupportedExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class S3CompatibilityTest {

        @LocalServerPort
        private int port;

        private S3Client s3Client;

        private static final String TEST_BUCKET = "test-bucket";
        private static final String TEST_BUCKET_2 = "test-bucket-2";
        private static final String TEST_KEY = "test-object.txt";
        private static final String TEST_CONTENT = "Hello, S3Chimera!";
        private static final String NON_EXISTENT_BUCKET = "non-existent-bucket-" + UUID.randomUUID();
        private static final String NON_EXISTENT_KEY = "non-existent-key-" + UUID.randomUUID() + ".txt";

        @BeforeAll
        void cleanupBeforeTests() {
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

                // 清理可能存在的测试数据
                cleanupBucketIfExists(TEST_BUCKET);
                cleanupBucketIfExists(TEST_BUCKET_2);
        }

        private void cleanupBucketIfExists(String bucketName) {
                try {
                        // First delete all objects in the bucket
                        ListObjectsV2Response listResponse = s3Client.listObjectsV2(b -> b.bucket(bucketName));
                        for (S3Object obj : listResponse.contents()) {
                                s3Client.deleteObject(b -> b.bucket(bucketName).key(obj.key()));
                        }
                        // 然后删除 bucket
                        s3Client.deleteBucket(b -> b.bucket(bucketName));
                } catch (NoSuchBucketException e) {
                        // Bucket 不存在，忽略
                } catch (Exception e) {
                        System.err.println("Failed to cleanup bucket " + bucketName + ": " + e.getMessage());
                }
        }

        /**
         * 确保 bucket 存在，如果不存在则创建
         */
        private void ensureBucketExists(String bucketName) {
                try {
                        s3Client.headBucket(b -> b.bucket(bucketName));
                } catch (NoSuchBucketException e) {
                        s3Client.createBucket(b -> b.bucket(bucketName));
                }
        }

        @BeforeEach
        void setUp() {
                // s3Client is initialized in @BeforeAll
        }

        @AfterAll
        void tearDownAll() {
                if (s3Client != null) {
                        s3Client.close();
                }
        }

        // ==================== Bucket 创建测试 ====================

        @Test
        @Order(1)
        @DisplayName("创建Bucket - 成功")
        void testCreateBucket_Success() {
                CreateBucketResponse response = s3Client.createBucket(b -> b.bucket(TEST_BUCKET));
                assertNotNull(response);
        }

        @Test
        @Order(2)
        @DisplayName("创建Bucket - 成功：重复创建同名Bucket（幂等操作）")
        void testCreateBucket_Success_Idempotent() {
                // Bucket was already created in Order(1)
                // Per AWS S3 spec: CreateBucket is idempotent for the same user, should return 200 OK
                CreateBucketResponse response = s3Client.createBucket(b -> b.bucket(TEST_BUCKET));
                assertNotNull(response);
        }

        @Test
        @Order(3)
        @DisplayName("创建第二个Bucket - 成功")
        void testCreateSecondBucket_Success() {
                CreateBucketResponse response = s3Client.createBucket(b -> b.bucket(TEST_BUCKET_2));
                assertNotNull(response);
        }

        // ==================== Bucket 列表测试 ====================

        @Test
        @Order(10)
        @DisplayName("ListBuckets - success: includes created buckets")
        void testListBuckets_Success() {
                ListBucketsResponse response = s3Client.listBuckets();
                assertNotNull(response.buckets());
                assertTrue(response.buckets().stream().anyMatch(b -> b.name().equals(TEST_BUCKET)));
                assertTrue(response.buckets().stream().anyMatch(b -> b.name().equals(TEST_BUCKET_2)));
        }

        // ==================== Bucket HEAD 测试 ====================

        @Test
        @Order(11)
        @DisplayName("检查Bucket存在 (HEAD) - 成功")
        void testHeadBucket_Success() {
                HeadBucketResponse response = s3Client.headBucket(b -> b.bucket(TEST_BUCKET));
                assertNotNull(response);
        }

        @Test
        @Order(12)
        @DisplayName("检查Bucket存在 (HEAD) - 失败：Bucket不存在")
        void testHeadBucket_Fail_NotFound() {
                S3Exception ex = assertThrows(S3Exception.class,
                                () -> s3Client.headBucket(b -> b.bucket(NON_EXISTENT_BUCKET)));
                assertEquals(404, ex.statusCode());
        }

        // ==================== Object PUT 测试 ====================

        @Test
        @Order(20)
        @DisplayName("上传对象 (PUT) - 成功")
        void testPutObject_Success() {
                PutObjectResponse response = s3Client.putObject(
                                b -> b.bucket(TEST_BUCKET).key(TEST_KEY).contentType("text/plain"),
                                RequestBody.fromString(TEST_CONTENT));
                assertNotNull(response.eTag());
        }

        @Test
        @Order(21)
        @DisplayName("上传对象 (PUT) - 失败：Bucket不存在")
        void testPutObject_Fail_BucketNotFound() {
                S3Exception ex = assertThrows(S3Exception.class, () -> s3Client.putObject(
                                b -> b.bucket(NON_EXISTENT_BUCKET).key(TEST_KEY),
                                RequestBody.fromString(TEST_CONTENT)));
                assertEquals(404, ex.statusCode());
        }

        @Test
        @Order(22)
        @DisplayName("Upload object (PUT) - success: overwrite existing object")
        void testPutObject_Success_Overwrite() {
                String newContent = "Updated content";
                PutObjectResponse response = s3Client.putObject(
                                b -> b.bucket(TEST_BUCKET).key(TEST_KEY).contentType("text/plain"),
                                RequestBody.fromString(newContent));
                assertNotNull(response.eTag());

                // Verify content has been updated
                var getResponse = s3Client.getObjectAsBytes(b -> b.bucket(TEST_BUCKET).key(TEST_KEY));
                assertEquals(newContent, getResponse.asString(StandardCharsets.UTF_8));

                // 恢复原始内容
                s3Client.putObject(
                                b -> b.bucket(TEST_BUCKET).key(TEST_KEY).contentType("text/plain"),
                                RequestBody.fromString(TEST_CONTENT));
        }

        // ==================== Object HEAD 测试 ====================

        @Test
        @Order(30)
        @DisplayName("获取对象元数据 (HEAD) - 成功")
        void testHeadObject_Success() {
                HeadObjectResponse response = s3Client.headObject(
                                b -> b.bucket(TEST_BUCKET).key(TEST_KEY));
                assertEquals("text/plain", response.contentType());
                assertEquals(TEST_CONTENT.length(), response.contentLength());
                assertNotNull(response.eTag());
        }

        @Test
        @Order(31)
        @DisplayName("获取对象元数据 (HEAD) - 失败：对象不存在")
        void testHeadObject_Fail_NotFound() {
                S3Exception ex = assertThrows(S3Exception.class,
                                () -> s3Client.headObject(b -> b.bucket(TEST_BUCKET).key(NON_EXISTENT_KEY)));
                assertEquals(404, ex.statusCode());
        }

        @Test
        @Order(32)
        @DisplayName("获取对象元数据 (HEAD) - 失败：Bucket不存在")
        void testHeadObject_Fail_BucketNotFound() {
                S3Exception ex = assertThrows(S3Exception.class,
                                () -> s3Client.headObject(b -> b.bucket(NON_EXISTENT_BUCKET).key(TEST_KEY)));
                assertEquals(404, ex.statusCode());
        }

        // ==================== Object GET 测试 ====================

        @Test
        @Order(40)
        @DisplayName("获取对象 (GET) - 成功")
        void testGetObject_Success() {
                var response = s3Client.getObjectAsBytes(b -> b.bucket(TEST_BUCKET).key(TEST_KEY));
                String content = response.asString(StandardCharsets.UTF_8);
                assertEquals(TEST_CONTENT, content);
        }

        @Test
        @Order(41)
        @DisplayName("获取对象 (GET) - 失败：对象不存在")
        void testGetObject_Fail_NotFound() {
                S3Exception ex = assertThrows(S3Exception.class,
                                () -> s3Client.getObjectAsBytes(b -> b.bucket(TEST_BUCKET).key(NON_EXISTENT_KEY)));
                assertEquals(404, ex.statusCode());
        }

        @Test
        @Order(42)
        @DisplayName("获取对象 (GET) - 失败：Bucket不存在")
        void testGetObject_Fail_BucketNotFound() {
                S3Exception ex = assertThrows(S3Exception.class,
                                () -> s3Client.getObjectAsBytes(b -> b.bucket(NON_EXISTENT_BUCKET).key(TEST_KEY)));
                assertEquals(404, ex.statusCode());
        }

        // ==================== Range GET 测试 ====================

        @Test
        @Order(50)
        @DisplayName("Range请求 (GET) - 成功：获取部分内容")
        void testGetObjectRange_Success() {
                // Upload longer content for Range testing
                String longContent = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
                s3Client.putObject(
                                b -> b.bucket(TEST_BUCKET).key("range-test.txt"),
                                RequestBody.fromString(longContent));

                // 获取 bytes 0-9
                var response = s3Client.getObject(
                                b -> b.bucket(TEST_BUCKET).key("range-test.txt").range("bytes=0-9"),
                                ResponseTransformer.toBytes());
                assertEquals("0123456789", response.asString(StandardCharsets.UTF_8));

                // 获取 bytes 10-19
                var response2 = s3Client.getObject(
                                b -> b.bucket(TEST_BUCKET).key("range-test.txt").range("bytes=10-19"),
                                ResponseTransformer.toBytes());
                assertEquals("ABCDEFGHIJ", response2.asString(StandardCharsets.UTF_8));

                // 清理
                s3Client.deleteObject(b -> b.bucket(TEST_BUCKET).key("range-test.txt"));
        }

        @Test
        @Order(51)
        @DisplayName("Range请求 (GET) - 失败：对象不存在")
        void testGetObjectRange_Fail_NotFound() {
                S3Exception ex = assertThrows(S3Exception.class, () -> s3Client.getObject(
                                b -> b.bucket(TEST_BUCKET).key(NON_EXISTENT_KEY).range("bytes=0-10"),
                                ResponseTransformer.toBytes()));
                assertEquals(404, ex.statusCode());
        }

        // ==================== ListObjects 测试 ====================

        @Test
        @Order(60)
        @DisplayName("列出对象 (ListObjectsV2) - 成功")
        void testListObjectsV2_Success() {
                ListObjectsV2Response response = s3Client.listObjectsV2(b -> b.bucket(TEST_BUCKET));
                assertNotNull(response.contents());
                assertTrue(response.contents().stream().anyMatch(obj -> obj.key().equals(TEST_KEY)));
        }

        @Test
        @Order(61)
        @DisplayName("ListObjectsV2 - success: filter by prefix")
        void testListObjectsV2_Success_WithPrefix() {
                // 上传多个对象
                s3Client.putObject(b -> b.bucket(TEST_BUCKET).key("folder/file1.txt"), RequestBody.fromString("1"));
                s3Client.putObject(b -> b.bucket(TEST_BUCKET).key("folder/file2.txt"), RequestBody.fromString("2"));
                s3Client.putObject(b -> b.bucket(TEST_BUCKET).key("other/file3.txt"), RequestBody.fromString("3"));

                ListObjectsV2Response response = s3Client.listObjectsV2(
                                b -> b.bucket(TEST_BUCKET).prefix("folder/"));

                assertEquals(2, response.contents().size());
                assertTrue(response.contents().stream().allMatch(obj -> obj.key().startsWith("folder/")));

                // 清理
                s3Client.deleteObject(b -> b.bucket(TEST_BUCKET).key("folder/file1.txt"));
                s3Client.deleteObject(b -> b.bucket(TEST_BUCKET).key("folder/file2.txt"));
                s3Client.deleteObject(b -> b.bucket(TEST_BUCKET).key("other/file3.txt"));
        }

        @Test
        @Order(62)
        @DisplayName("列出对象 (ListObjectsV2) - 失败：Bucket不存在")
        void testListObjectsV2_Fail_BucketNotFound() {
                S3Exception ex = assertThrows(S3Exception.class,
                                () -> s3Client.listObjectsV2(b -> b.bucket(NON_EXISTENT_BUCKET)));
                assertEquals(404, ex.statusCode());
        }

        @Test
        @Order(63)
        @DisplayName("列出对象 (ListObjectsV2) - 成功：空结果（前缀不匹配）")
        void testListObjectsV2_Success_EmptyResult() {
                ListObjectsV2Response response = s3Client.listObjectsV2(
                                b -> b.bucket(TEST_BUCKET).prefix("nonexistent-prefix/"));
                // If there are objects, print them for debugging
                if (!response.contents().isEmpty()) {
                        System.out.println("Unexpected objects found with prefix 'nonexistent-prefix/':");
                        response.contents().forEach(obj -> System.out.println("  - " + obj.key()));
                }
                assertTrue(response.contents().isEmpty(),
                                "Expected empty result for non-matching prefix, but found: "
                                                + response.contents().size() + " objects");
        }

        // ==================== Copy Object 测试 ====================

        @Test
        @Order(70)
        @DisplayName("复制对象 (COPY) - 成功：同Bucket内复制")
        void testCopyObject_Success_SameBucket() {
                String destKey = "copied-object.txt";
                CopyObjectResponse response = s3Client.copyObject(b -> b
                                .sourceBucket(TEST_BUCKET)
                                .sourceKey(TEST_KEY)
                                .destinationBucket(TEST_BUCKET)
                                .destinationKey(destKey));
                assertNotNull(response.copyObjectResult());

                // 验证复制成功
                var getResponse = s3Client.getObjectAsBytes(b -> b.bucket(TEST_BUCKET).key(destKey));
                assertEquals(TEST_CONTENT, getResponse.asString(StandardCharsets.UTF_8));

                // 清理
                s3Client.deleteObject(b -> b.bucket(TEST_BUCKET).key(destKey));
        }

        @Test
        @Order(71)
        @DisplayName("复制对象 (COPY) - 失败：源对象不存在")
        void testCopyObject_Fail_SourceNotFound() {
                S3Exception ex = assertThrows(S3Exception.class, () -> s3Client.copyObject(b -> b
                                .sourceBucket(TEST_BUCKET)
                                .sourceKey(NON_EXISTENT_KEY)
                                .destinationBucket(TEST_BUCKET)
                                .destinationKey("dest.txt")));
                assertEquals(404, ex.statusCode());
        }

        @Test
        @Order(72)
        @DisplayName("复制对象 (COPY) - 失败：源Bucket不存在")
        void testCopyObject_Fail_SourceBucketNotFound() {
                S3Exception ex = assertThrows(S3Exception.class, () -> s3Client.copyObject(b -> b
                                .sourceBucket(NON_EXISTENT_BUCKET)
                                .sourceKey(TEST_KEY)
                                .destinationBucket(TEST_BUCKET)
                                .destinationKey("dest.txt")));
                assertEquals(404, ex.statusCode());
        }

        // ==================== Batch Delete 测试 ====================

        @Test
        @Order(80)
        @DisplayName("批量删除对象 - 成功")
        void testDeleteObjects_Success() {
                // 创建测试对象
                s3Client.putObject(b -> b.bucket(TEST_BUCKET).key("batch/1.txt"), RequestBody.fromString("1"));
                s3Client.putObject(b -> b.bucket(TEST_BUCKET).key("batch/2.txt"), RequestBody.fromString("2"));
                s3Client.putObject(b -> b.bucket(TEST_BUCKET).key("batch/3.txt"), RequestBody.fromString("3"));

                List<ObjectIdentifier> toDelete = List.of(
                                ObjectIdentifier.builder().key("batch/1.txt").build(),
                                ObjectIdentifier.builder().key("batch/2.txt").build(),
                                ObjectIdentifier.builder().key("batch/3.txt").build());

                DeleteObjectsResponse response = s3Client.deleteObjects(b -> b
                                .bucket(TEST_BUCKET)
                                .delete(d -> d.objects(toDelete)));

                assertEquals(3, response.deleted().size(),
                                "Expected 3 objects deleted, but got: " + response.deleted().size());

                // Verify object is deleted
                ListObjectsV2Response listResponse = s3Client.listObjectsV2(
                                b -> b.bucket(TEST_BUCKET).prefix("batch/"));
                // If there are objects, print them for debugging
                if (!listResponse.contents().isEmpty()) {
                        System.out.println("Unexpected objects found after batch delete with prefix 'batch/':");
                        listResponse.contents().forEach(obj -> System.out.println("  - " + obj.key()));
                }
                assertTrue(listResponse.contents().isEmpty(),
                                "Expected empty after deletion, but found: " + listResponse.contents().size()
                                                + " objects");
        }

        @Test
        @Order(81)
        @DisplayName("批量删除对象 - 成功：部分对象不存在（静默处理）")
        void testDeleteObjects_Success_PartialNotFound() {
                // Create a test object
                s3Client.putObject(b -> b.bucket(TEST_BUCKET).key("partial/exist.txt"), RequestBody.fromString("1"));

                List<ObjectIdentifier> toDelete = List.of(
                                ObjectIdentifier.builder().key("partial/exist.txt").build(),
                                ObjectIdentifier.builder().key("partial/not-exist.txt").build());

                // Batch delete should succeed, silently ignoring non-existent objects
                DeleteObjectsResponse response = s3Client.deleteObjects(b -> b
                                .bucket(TEST_BUCKET)
                                .delete(d -> d.objects(toDelete)));

                // Both objects should be reported as deleted (S3 behavior)
                assertFalse(response.deleted().isEmpty());
        }

        @Test
        @Order(82)
        @DisplayName("批量删除对象 - 失败：Bucket不存在")
        void testDeleteObjects_Fail_BucketNotFound() {
                List<ObjectIdentifier> toDelete = List.of(
                                ObjectIdentifier.builder().key("any.txt").build());

                S3Exception ex = assertThrows(S3Exception.class, () -> s3Client.deleteObjects(b -> b
                                .bucket(NON_EXISTENT_BUCKET)
                                .delete(d -> d.objects(toDelete))));
                assertEquals(404, ex.statusCode());
        }

        // ==================== Multipart Upload 测试 ====================

        @Test
        @Order(90)
        @DisplayName("分片上传 - 成功：完整流程")
        void testMultipartUpload_Success() {
                // 确保 bucket 存在
                ensureBucketExists(TEST_BUCKET);

                String multipartKey = "multipart-test.bin";

                // 1. 初始化分片上传
                CreateMultipartUploadResponse createResponse = s3Client.createMultipartUpload(
                                b -> b.bucket(TEST_BUCKET).key(multipartKey).contentType("application/octet-stream"));
                String uploadId = createResponse.uploadId();
                assertNotNull(uploadId);

                // 2. Upload parts (S3 minimum part size 5MB, last part can be smaller)
                List<CompletedPart> completedParts = new ArrayList<>();

                // Part 1: 5MB (S3 minimum part size)
                byte[] part1Data = new byte[5 * 1024 * 1024];
                java.util.Arrays.fill(part1Data, (byte) 'A');
                UploadPartResponse part1Response = s3Client.uploadPart(
                                b -> b.bucket(TEST_BUCKET).key(multipartKey).uploadId(uploadId).partNumber(1),
                                RequestBody.fromBytes(part1Data));
                completedParts.add(CompletedPart.builder().partNumber(1).eTag(part1Response.eTag()).build());

                // Part 2: can be smaller than 5MB (last part)
                byte[] part2Data = new byte[1024];
                java.util.Arrays.fill(part2Data, (byte) 'B');
                UploadPartResponse part2Response = s3Client.uploadPart(
                                b -> b.bucket(TEST_BUCKET).key(multipartKey).uploadId(uploadId).partNumber(2),
                                RequestBody.fromBytes(part2Data));
                completedParts.add(CompletedPart.builder().partNumber(2).eTag(part2Response.eTag()).build());

                // 3. 完成分片上传
                CompleteMultipartUploadResponse completeResponse = s3Client.completeMultipartUpload(b -> b
                                .bucket(TEST_BUCKET)
                                .key(multipartKey)
                                .uploadId(uploadId)
                                .multipartUpload(m -> m.parts(completedParts)));
                assertNotNull(completeResponse.eTag());
                // Multipart upload ETag should be in md5-partCount format
                assertTrue(completeResponse.eTag().contains("-"));

                // 4. 验证内容
                var getResponse = s3Client.getObjectAsBytes(b -> b.bucket(TEST_BUCKET).key(multipartKey));
                byte[] downloadedContent = getResponse.asByteArray();
                assertEquals(part1Data.length + part2Data.length, downloadedContent.length);

                // 清理
                s3Client.deleteObject(b -> b.bucket(TEST_BUCKET).key(multipartKey));
        }

        @Test
        @Order(91)
        @DisplayName("分片上传 - 成功：中止上传")
        void testMultipartUpload_Success_Abort() {
                String multipartKey = "abort-test.bin";

                // 初始化分片上传
                CreateMultipartUploadResponse createResponse = s3Client.createMultipartUpload(
                                b -> b.bucket(TEST_BUCKET).key(multipartKey));
                String uploadId = createResponse.uploadId();
                assertNotNull(uploadId);

                // Upload a part
                s3Client.uploadPart(
                                b -> b.bucket(TEST_BUCKET).key(multipartKey).uploadId(uploadId).partNumber(1),
                                RequestBody.fromString("Partial data"));

                // 中止上传
                AbortMultipartUploadResponse abortResponse = s3Client.abortMultipartUpload(b -> b
                                .bucket(TEST_BUCKET)
                                .key(multipartKey)
                                .uploadId(uploadId));
                assertNotNull(abortResponse);

                // 验证对象不存在
                S3Exception ex = assertThrows(S3Exception.class,
                                () -> s3Client.headObject(b -> b.bucket(TEST_BUCKET).key(multipartKey)));
                assertEquals(404, ex.statusCode());
        }

        @Test
        @Order(92)
        @DisplayName("分片上传 - 失败：无效的 uploadId")
        void testMultipartUpload_Fail_InvalidUploadId() {
                assertThrows(Exception.class, () -> s3Client.uploadPart(
                                b -> b.bucket(TEST_BUCKET)
                                                .key("any.txt")
                                                .uploadId("invalid-upload-id-" + UUID.randomUUID())
                                                .partNumber(1),
                                RequestBody.fromString("data")));
        }

        @Test
        @Order(93)
        @DisplayName("列出分片上传 - 成功")
        void testListMultipartUploads_Success() {
                String key = "list-parts-test.bin";

                // Create a multipart upload
                CreateMultipartUploadResponse createResponse = s3Client.createMultipartUpload(
                                b -> b.bucket(TEST_BUCKET).key(key));
                String uploadId = createResponse.uploadId();

                // 列出进行中的分片上传
                ListMultipartUploadsResponse listResponse = s3Client.listMultipartUploads(
                                b -> b.bucket(TEST_BUCKET));
                assertTrue(listResponse.uploads().stream()
                                .anyMatch(u -> u.uploadId().equals(uploadId)));

                // 清理 - 中止上传
                s3Client.abortMultipartUpload(b -> b.bucket(TEST_BUCKET).key(key).uploadId(uploadId));
        }

        // ==================== ACL 测试 ====================

        @Test
        @Order(100)
        @DisplayName("获取Bucket ACL - 成功")
        void testGetBucketAcl_Success() {
                GetBucketAclResponse response = s3Client.getBucketAcl(b -> b.bucket(TEST_BUCKET));
                assertNotNull(response.owner());
                assertNotNull(response.grants());
        }

        @Test
        @Order(101)
        @DisplayName("获取Bucket ACL - 失败：Bucket不存在")
        void testGetBucketAcl_Fail_BucketNotFound() {
                S3Exception ex = assertThrows(S3Exception.class,
                                () -> s3Client.getBucketAcl(b -> b.bucket(NON_EXISTENT_BUCKET)));
                assertEquals(404, ex.statusCode());
        }

        @Test
        @Order(102)
        @DisplayName("获取Object ACL - 成功")
        void testGetObjectAcl_Success() {
                GetObjectAclResponse response = s3Client.getObjectAcl(
                                b -> b.bucket(TEST_BUCKET).key(TEST_KEY));
                assertNotNull(response.owner());
                assertNotNull(response.grants());
        }

        @Test
        @Order(103)
        @DisplayName("获取Object ACL - 失败：对象不存在")
        void testGetObjectAcl_Fail_ObjectNotFound() {
                S3Exception ex = assertThrows(S3Exception.class,
                                () -> s3Client.getObjectAcl(b -> b.bucket(TEST_BUCKET).key(NON_EXISTENT_KEY)));
                assertEquals(404, ex.statusCode());
        }

        // ==================== Bucket Location 测试 ====================

        @Test
        @Order(110)
        @DisplayName("获取Bucket Location - 成功")
        void testGetBucketLocation_Success() {
                GetBucketLocationResponse response = s3Client.getBucketLocation(
                                b -> b.bucket(TEST_BUCKET));
                // Should return configured region or null/empty (for us-east-1)
                assertNotNull(response);
        }

        @Test
        @Order(111)
        @DisplayName("获取Bucket Location - 失败：Bucket不存在")
        void testGetBucketLocation_Fail_BucketNotFound() {
                S3Exception ex = assertThrows(S3Exception.class,
                                () -> s3Client.getBucketLocation(b -> b.bucket(NON_EXISTENT_BUCKET)));
                assertEquals(404, ex.statusCode());
        }

        // ==================== 删除对象测试 ====================

        @Test
        @Order(200)
        @DisplayName("删除对象 - 成功")
        void testDeleteObject_Success() {
                // 创建测试对象
                s3Client.putObject(
                                b -> b.bucket(TEST_BUCKET).key("delete-test.txt"),
                                RequestBody.fromString("to be deleted"));

                // 删除
                s3Client.deleteObject(b -> b.bucket(TEST_BUCKET).key("delete-test.txt"));

                // Verify object is deleted
                S3Exception ex = assertThrows(S3Exception.class,
                                () -> s3Client.headObject(b -> b.bucket(TEST_BUCKET).key("delete-test.txt")));
                assertEquals(404, ex.statusCode());
        }

        @Test
        @Order(201)
        @DisplayName("删除对象 - 成功：删除不存在的对象（幂等操作）")
        void testDeleteObject_Success_NotExist() {
                // S3 deleting a non-existent object is idempotent and should not throw
                assertDoesNotThrow(() -> s3Client
                                .deleteObject(b -> b.bucket(TEST_BUCKET).key("not-exist-" + UUID.randomUUID())));
        }

        @Test
        @Order(202)
        @DisplayName("删除对象 - 失败：Bucket不存在")
        void testDeleteObject_Fail_BucketNotFound() {
                S3Exception ex = assertThrows(S3Exception.class,
                                () -> s3Client.deleteObject(b -> b.bucket(NON_EXISTENT_BUCKET).key("any.txt")));
                assertEquals(404, ex.statusCode());
        }

        // ==================== 删除 Bucket 测试 ====================

        @Test
        @Order(900)
        @DisplayName("删除Bucket - 失败：Bucket非空")
        void testDeleteBucket_Fail_NotEmpty() {
                // Ensure bucket has objects
                s3Client.putObject(
                                b -> b.bucket(TEST_BUCKET).key("blocker.txt"),
                                RequestBody.fromString("blocking deletion"));

                assertThrows(S3Exception.class, () -> s3Client.deleteBucket(b -> b.bucket(TEST_BUCKET)));

                // 清理
                s3Client.deleteObject(b -> b.bucket(TEST_BUCKET).key("blocker.txt"));
        }

        @Test
        @Order(998)
        @DisplayName("Cleanup: delete all test objects")
        void cleanupObjects() {
                // Delete all objects in TEST_BUCKET
                try {
                        ListObjectsV2Response listResponse = s3Client.listObjectsV2(b -> b.bucket(TEST_BUCKET));
                        for (S3Object obj : listResponse.contents()) {
                                s3Client.deleteObject(b -> b.bucket(TEST_BUCKET).key(obj.key()));
                        }
                } catch (Exception ignored) {
                }

                // Delete all objects in TEST_BUCKET_2
                try {
                        ListObjectsV2Response listResponse = s3Client.listObjectsV2(b -> b.bucket(TEST_BUCKET_2));
                        for (S3Object obj : listResponse.contents()) {
                                s3Client.deleteObject(b -> b.bucket(TEST_BUCKET_2).key(obj.key()));
                        }
                } catch (Exception ignored) {
                }
        }

        @Test
        @Order(999)
        @DisplayName("删除Bucket - 成功：删除空Bucket")
        void testDeleteBucket_Success() {
                // 确保 Bucket 存在（如果之前被清理或未创建）
                ensureBucketExists(TEST_BUCKET);
                ensureBucketExists(TEST_BUCKET_2);

                // Delete the first bucket
                s3Client.deleteBucket(b -> b.bucket(TEST_BUCKET));
                S3Exception ex1 = assertThrows(S3Exception.class,
                                () -> s3Client.headBucket(b -> b.bucket(TEST_BUCKET)));
                assertEquals(404, ex1.statusCode());

                // 删除第二个 Bucket
                s3Client.deleteBucket(b -> b.bucket(TEST_BUCKET_2));
                S3Exception ex2 = assertThrows(S3Exception.class,
                                () -> s3Client.headBucket(b -> b.bucket(TEST_BUCKET_2)));
                assertEquals(404, ex2.statusCode());
        }

        @Test
        @Order(1000)
        @DisplayName("删除Bucket - 失败：Bucket不存在")
        void testDeleteBucket_Fail_NotFound() {
                S3Exception ex = assertThrows(S3Exception.class,
                                () -> s3Client.deleteBucket(b -> b.bucket(NON_EXISTENT_BUCKET)));
                assertEquals(404, ex.statusCode());
        }
}
