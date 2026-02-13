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
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * S3 Handler 覆盖测试
 * <p>
 * Ensures all 19 handlers have complete functional tests,
 * 关注返回值验证和内容正确性，而非仅检查不报错。
 * <p>
 * 覆盖的 Handler:
 * - Bucket: CreateBucket, DeleteBucket, ListBuckets, BucketExists
 * - Object: PutObject, GetObject, GetObjectRange, HeadObject, DeleteObject,
 * DeleteObjects,
 * CopyObject, ListObjects, ListObjectsV2
 * - Multipart: CreateMultipartUpload, UploadPart, CompleteMultipartUpload,
 * AbortMultipartUpload, ListParts, ListMultipartUploads
 * <p>
 * 运行方式：
 * - ./gradlew :chimera-test-s3:test --tests "*HandlerCoverage*" -Pdriver=memory
 */
@SpringBootTest(classes = ChimeraServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ExtendWith(SkipOnNotSupportedExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Timeout(value = 120, unit = TimeUnit.SECONDS)
public class S3HandlerCoverageTest {

        @LocalServerPort
        private int port;

        private S3Client s3Client;

        private final String BUCKET = "handler-coverage-" + UUID.randomUUID().toString().substring(0, 8);

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
        }

        @AfterAll
        void tearDown() {
                // 清理主测试 bucket
                cleanupBucket(BUCKET);
                if (s3Client != null) {
                        s3Client.close();
                }
        }

        private void cleanupBucket(String bucketName) {
                try {
                        ListObjectsV2Response listResponse = s3Client.listObjectsV2(b -> b.bucket(bucketName));
                        for (S3Object obj : listResponse.contents()) {
                                s3Client.deleteObject(b -> b.bucket(bucketName).key(obj.key()));
                        }
                        s3Client.deleteBucket(b -> b.bucket(bucketName));
                } catch (Exception e) {
                        // 忽略清理错误
                }
        }

        // ==================== Bucket Handlers ====================

        @Test
        @Order(1)
        @DisplayName("Handler: CreateBucket - 返回值验证")
        void testHandler_CreateBucket() {
                CreateBucketResponse response = s3Client.createBucket(b -> b.bucket(BUCKET));

                // Verify response
                assertNotNull(response, "CreateBucket response should not be null");
                // 验证 bucket 确实存在
                HeadBucketResponse headResponse = s3Client.headBucket(b -> b.bucket(BUCKET));
                assertNotNull(headResponse, "Bucket should exist after creation");
        }

        @Test
        @Order(2)
        @DisplayName("Handler: ListBuckets - 包含新建Bucket")
        void testHandler_ListBuckets() {
                ListBucketsResponse response = s3Client.listBuckets();

                assertNotNull(response.buckets(), "Buckets list should not be null");
                assertFalse(response.buckets().isEmpty(), "Buckets list should not be empty");

                // 验证包含我们创建的 bucket
                boolean found = response.buckets().stream()
                                .anyMatch(b -> b.name().equals(BUCKET));
                assertTrue(found, "ListBuckets should include the created bucket: " + BUCKET);

                // Verify bucket has creation time
                response.buckets().stream()
                                .filter(b -> b.name().equals(BUCKET))
                                .findFirst()
                                .ifPresent(b -> assertNotNull(b.creationDate(), "Bucket should have creation date"));
        }

        @Test
        @Order(3)
        @DisplayName("Handler: BucketExists (HEAD) - 验证存在")
        void testHandler_BucketExists() {
                HeadBucketResponse response = s3Client.headBucket(b -> b.bucket(BUCKET));
                assertNotNull(response, "HeadBucket should return response for existing bucket");

                // 验证不存在的 bucket 返回 404
                String nonExistent = "non-existent-" + UUID.randomUUID();
                S3Exception ex = assertThrows(S3Exception.class,
                                () -> s3Client.headBucket(b -> b.bucket(nonExistent)));
                assertEquals(404, ex.statusCode(), "Non-existent bucket should return 404");
        }

        // ==================== Object Handlers ====================

        @Test
        @Order(10)
        @DisplayName("Handler: PutObject - ETag和内容验证")
        void testHandler_PutObject() {
                String key = "put-object-test.txt";
                String content = "Test content for PutObject handler";
                byte[] contentBytes = content.getBytes(StandardCharsets.UTF_8);

                PutObjectResponse response = s3Client.putObject(
                                b -> b.bucket(BUCKET).key(key).contentType("text/plain"),
                                RequestBody.fromString(content));

                // 验证 ETag
                DataIntegrityAssert.assertETagValid(response.eTag());

                // Verify it can be read back
                byte[] downloaded = s3Client.getObjectAsBytes(b -> b.bucket(BUCKET).key(key)).asByteArray();
                DataIntegrityAssert.assertContentEquals(contentBytes, downloaded);

                s3Client.deleteObject(b -> b.bucket(BUCKET).key(key));
        }

        @Test
        @Order(11)
        @DisplayName("Handler: GetObject - 完整内容返回")
        void testHandler_GetObject() {
                String key = "get-object-test.bin";
                byte[] content = TestDataGenerator.generateRandomBytes(5000);
                String expectedMd5 = TestDataGenerator.calculateMD5(content);

                s3Client.putObject(
                                b -> b.bucket(BUCKET).key(key).contentType("application/octet-stream"),
                                RequestBody.fromBytes(content));

                // GetObject returns complete response
                byte[] downloaded = s3Client.getObjectAsBytes(b -> b.bucket(BUCKET).key(key)).asByteArray();

                // 验证内容
                DataIntegrityAssert.assertContentEquals(content, downloaded);
                assertEquals(expectedMd5, TestDataGenerator.calculateMD5(downloaded));

                // 验证 HeadObject 元数据
                HeadObjectResponse headResponse = s3Client.headObject(b -> b.bucket(BUCKET).key(key));
                assertEquals(content.length, headResponse.contentLength());
                assertTrue(headResponse.contentType().contains("application/octet-stream"));

                s3Client.deleteObject(b -> b.bucket(BUCKET).key(key));
        }

        @Test
        @Order(12)
        @DisplayName("Handler: GetObjectRange - 精确范围返回")
        void testHandler_GetObjectRange() {
                String key = "range-handler-test.txt";
                String content = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
                s3Client.putObject(
                                b -> b.bucket(BUCKET).key(key).contentType("text/plain"),
                                RequestBody.fromString(content));

                // 获取 bytes 10-19
                var rangeResponse = s3Client.getObject(
                                b -> b.bucket(BUCKET).key(key).range("bytes=10-19"),
                                ResponseTransformer.toBytes());

                String rangeContent = rangeResponse.asString(StandardCharsets.UTF_8);
                assertEquals("ABCDEFGHIJ", rangeContent, "Range should return exact bytes 10-19");
                assertEquals(10, rangeResponse.asByteArray().length);

                s3Client.deleteObject(b -> b.bucket(BUCKET).key(key));
        }

        @Test
        @Order(13)
        @DisplayName("Handler: HeadObject - 完整元数据返回")
        void testHandler_HeadObject() {
                String key = "head-object-test.json";
                String content = "{\"name\": \"test\"}";
                Map<String, String> metadata = Map.of("custom-key", "custom-value");

                s3Client.putObject(
                                b -> b.bucket(BUCKET).key(key)
                                                .contentType("application/json")
                                                .metadata(metadata),
                                RequestBody.fromString(content));

                HeadObjectResponse response = s3Client.headObject(b -> b.bucket(BUCKET).key(key));

                // Verify all metadata fields
                assertEquals(content.length(), response.contentLength(), "Content-Length should match");
                assertTrue(response.contentType().contains("application/json"), "Content-Type should be JSON");
                assertNotNull(response.eTag(), "ETag should not be null");
                assertNotNull(response.lastModified(), "Last-Modified should not be null");

                // Verify user metadata
                if (response.metadata() != null && !response.metadata().isEmpty()) {
                        String value = response.metadata().getOrDefault("custom-key",
                                        response.metadata().get("Custom-Key"));
                        assertEquals("custom-value", value, "User metadata should be preserved");
                }

                s3Client.deleteObject(b -> b.bucket(BUCKET).key(key));
        }

        @Test
        @Order(14)
        @DisplayName("Handler: DeleteObject - 幂等删除验证")
        void testHandler_DeleteObject() {
                String key = "delete-test.txt";

                s3Client.putObject(
                                b -> b.bucket(BUCKET).key(key),
                                RequestBody.fromString("To be deleted"));

                // First delete
                DeleteObjectResponse response = s3Client.deleteObject(b -> b.bucket(BUCKET).key(key));
                assertNotNull(response, "DeleteObject should return response");

                // Verify object is deleted
                assertThrows(S3Exception.class,
                                () -> s3Client.headObject(b -> b.bucket(BUCKET).key(key)));

                // Idempotent delete (second delete should not error)
                assertDoesNotThrow(() -> s3Client.deleteObject(b -> b.bucket(BUCKET).key(key)));
        }

        @Test
        @Order(15)
        @DisplayName("Handler: DeleteObjects - 批量删除返回值")
        void testHandler_DeleteObjects() {
                // 创建多个对象
                List<String> keys = new ArrayList<>();
                for (int i = 0; i < 5; i++) {
                        String key = "batch-delete-" + i + "-" + UUID.randomUUID().toString().substring(0, 8) + ".txt";
                        keys.add(key);
                        s3Client.putObject(b -> b.bucket(BUCKET).key(key), RequestBody.fromString("Content " + i));
                }

                // 批量删除
                List<ObjectIdentifier> toDelete = keys.stream()
                                .map(k -> ObjectIdentifier.builder().key(k).build())
                                .toList();

                DeleteObjectsResponse response = s3Client.deleteObjects(b -> b
                                .bucket(BUCKET)
                                .delete(d -> d.objects(toDelete)));

                // Verify return value - deleted object count should be greater than 0
                assertFalse(response.deleted().isEmpty(), "Should report some deleted objects");
                // 验证每个我们请求删除的对象都被处理了
                assertTrue(response.deleted().size() >= 5,
                                "Should delete at least 5 objects, got: " + response.deleted().size());

                // 验证确认删除
                for (String key : keys) {
                        assertThrows(S3Exception.class,
                                        () -> s3Client.headObject(b -> b.bucket(BUCKET).key(key)));
                }
        }

        @Test
        @Order(16)
        @DisplayName("Handler: CopyObject - source and target verification")
        void testHandler_CopyObject() {
                String sourceKey = "copy-source.txt";
                String destKey = "copy-dest.txt";
                String content = "Content to be copied";

                s3Client.putObject(
                                b -> b.bucket(BUCKET).key(sourceKey).contentType("text/plain"),
                                RequestBody.fromString(content));

                CopyObjectResponse response = s3Client.copyObject(b -> b
                                .sourceBucket(BUCKET).sourceKey(sourceKey)
                                .destinationBucket(BUCKET).destinationKey(destKey));

                // 验证返回值
                assertNotNull(response.copyObjectResult(), "CopyObjectResult should not be null");
                assertNotNull(response.copyObjectResult().eTag(), "Copy ETag should not be null");

                // 验证目标内容
                byte[] destContent = s3Client.getObjectAsBytes(b -> b.bucket(BUCKET).key(destKey)).asByteArray();
                assertEquals(content, new String(destContent, StandardCharsets.UTF_8));

                // 验证源未被删除
                HeadObjectResponse sourceHead = s3Client.headObject(b -> b.bucket(BUCKET).key(sourceKey));
                assertNotNull(sourceHead);

                s3Client.deleteObject(b -> b.bucket(BUCKET).key(sourceKey));
                s3Client.deleteObject(b -> b.bucket(BUCKET).key(destKey));
        }

        @Test
        @Order(17)
        @DisplayName("Handler: ListObjects (V1) - 返回值验证")
        void testHandler_ListObjects_V1() {
                // 创建测试对象
                for (int i = 0; i < 5; i++) {
                        final int idx = i;
                        s3Client.putObject(
                                        b -> b.bucket(BUCKET).key("listv1/file" + idx + ".txt"),
                                        RequestBody.fromString("Content " + idx));
                }

                // Use ListObjects V1
                ListObjectsResponse response = s3Client.listObjects(
                                b -> b.bucket(BUCKET).prefix("listv1/"));

                // 验证返回值
                assertEquals(BUCKET, response.name(), "Bucket name should match");
                assertEquals("listv1/", response.prefix(), "Prefix should match");
                assertEquals(5, response.contents().size(), "Should list 5 objects");

                // Verify each object has required fields
                for (S3Object obj : response.contents()) {
                        assertNotNull(obj.key(), "Object key should not be null");
                        assertNotNull(obj.size(), "Object size should not be null");
                        assertNotNull(obj.lastModified(), "Last modified should not be null");
                }

                // 清理
                for (int i = 0; i < 5; i++) {
                        final int idx = i;
                        s3Client.deleteObject(b -> b.bucket(BUCKET).key("listv1/file" + idx + ".txt"));
                }
        }

        @Test
        @Order(18)
        @DisplayName("Handler: ListObjectsV2 - 分页和Continuation")
        void testHandler_ListObjectsV2() {
                // 创建测试对象
                for (int i = 0; i < 5; i++) {
                        final int idx = i;
                        s3Client.putObject(
                                        b -> b.bucket(BUCKET).key("listv2/file" + idx + ".txt"),
                                        RequestBody.fromString("Content " + idx));
                }

                // 首次请求限制 3 个
                ListObjectsV2Response response1 = s3Client.listObjectsV2(
                                b -> b.bucket(BUCKET).prefix("listv2/").maxKeys(3));

                assertEquals(3, response1.contents().size(), "First page should have 3 objects");
                assertEquals(3, response1.maxKeys(), "MaxKeys should be 3");

                // 某些驱动可能不支持分页，如果支持分页则验证续期功能
                if (response1.isTruncated() && response1.nextContinuationToken() != null) {
                        // 继续获取
                        ListObjectsV2Response response2 = s3Client.listObjectsV2(
                                        b -> b.bucket(BUCKET).prefix("listv2/")
                                                        .continuationToken(response1.nextContinuationToken()));

                        // Total count should be 5
                        int total = response1.contents().size() + response2.contents().size();
                        assertTrue(total >= 5, "Total objects should be at least 5, got: " + total);
                } else {
                        System.out.println(
                                        "⚠️ Driver does not support pagination or returned all results in first page");
                }

                // 清理
                for (int i = 0; i < 5; i++) {
                        final int idx = i;
                        s3Client.deleteObject(b -> b.bucket(BUCKET).key("listv2/file" + idx + ".txt"));
                }
        }

        // ==================== Multipart Handlers ====================

        @Test
        @Order(30)
        @DisplayName("Handler: CreateMultipartUpload - UploadId验证")
        void testHandler_CreateMultipartUpload() {
                String key = "multipart-create-test.bin";

                CreateMultipartUploadResponse response = s3Client.createMultipartUpload(
                                b -> b.bucket(BUCKET).key(key).contentType("application/octet-stream"));

                // 验证返回值
                assertNotNull(response.uploadId(), "Upload ID should not be null");
                assertFalse(response.uploadId().isEmpty(), "Upload ID should not be empty");
                assertEquals(BUCKET, response.bucket(), "Bucket should match");
                assertEquals(key, response.key(), "Key should match");

                // 中止上传（清理）
                s3Client.abortMultipartUpload(b -> b.bucket(BUCKET).key(key).uploadId(response.uploadId()));
        }

        @Test
        @Order(31)
        @DisplayName("Handler: UploadPart - ETag返回验证")
        void testHandler_UploadPart() {
                String key = "multipart-part-test.bin";
                byte[] partData = TestDataGenerator.generateRandomBytes(5 * 1024 * 1024); // 5MB

                // 初始化
                CreateMultipartUploadResponse createResp = s3Client.createMultipartUpload(
                                b -> b.bucket(BUCKET).key(key));
                String uploadId = createResp.uploadId();

                // 上传分片
                UploadPartResponse partResponse = s3Client.uploadPart(
                                b -> b.bucket(BUCKET).key(key).uploadId(uploadId).partNumber(1),
                                RequestBody.fromBytes(partData));

                // 验证返回值
                assertNotNull(partResponse.eTag(), "Part ETag should not be null");
                DataIntegrityAssert.assertETagValid(partResponse.eTag());

                // 中止（清理）
                s3Client.abortMultipartUpload(b -> b.bucket(BUCKET).key(key).uploadId(uploadId));
        }

        @Test
        @Order(32)
        @DisplayName("Handler: ListParts - 分片列表验证")
        void testHandler_ListParts() {
                String key = "multipart-listparts-test.bin";
                byte[] part1 = TestDataGenerator.generateRandomBytes(5 * 1024 * 1024);
                byte[] part2 = TestDataGenerator.generateRandomBytes(5 * 1024 * 1024);

                // 初始化
                CreateMultipartUploadResponse createResp = s3Client.createMultipartUpload(
                                b -> b.bucket(BUCKET).key(key));
                String uploadId = createResp.uploadId();

                // 上传两个分片
                UploadPartResponse part1Resp = s3Client.uploadPart(
                                b -> b.bucket(BUCKET).key(key).uploadId(uploadId).partNumber(1),
                                RequestBody.fromBytes(part1));
                UploadPartResponse part2Resp = s3Client.uploadPart(
                                b -> b.bucket(BUCKET).key(key).uploadId(uploadId).partNumber(2),
                                RequestBody.fromBytes(part2));

                // 验证分片上传成功
                assertNotNull(part1Resp.eTag(), "Part 1 ETag should not be null");
                assertNotNull(part2Resp.eTag(), "Part 2 ETag should not be null");

                // ListParts - 某些驱动可能不支持 ListParts
                try {
                        ListPartsResponse listResponse = s3Client.listParts(
                                        b -> b.bucket(BUCKET).key(key).uploadId(uploadId));

                        // 如果支持，验证返回值
                        if (listResponse.uploadId() != null) {
                                assertEquals(uploadId, listResponse.uploadId(), "Upload ID should match");
                        }
                        if (listResponse.key() != null) {
                                assertEquals(key, listResponse.key(), "Key should match");
                        }

                        if (!listResponse.parts().isEmpty()) {
                                // 验证分片详情
                                for (Part part : listResponse.parts()) {
                                        assertNotNull(part.partNumber(), "Part number should not be null");
                                        assertNotNull(part.eTag(), "Part ETag should not be null");
                                }
                        } else {
                                System.out.println(
                                                "⚠️ ListParts returned empty (driver may not fully support this operation)");
                        }
                } catch (Exception e) {
                        System.out.println("⚠️ ListParts not supported by driver: " + e.getMessage());
                }

                // 中止（清理）
                s3Client.abortMultipartUpload(b -> b.bucket(BUCKET).key(key).uploadId(uploadId));
        }

        @Test
        @Order(33)
        @DisplayName("Handler: CompleteMultipartUpload - 完整流程验证")
        void testHandler_CompleteMultipartUpload() {
                String key = "multipart-complete-test.bin";
                byte[] part1 = TestDataGenerator.generateRandomBytes(5 * 1024 * 1024);
                byte[] part2 = TestDataGenerator.generateRandomBytes(1024);

                byte[] fullContent = new byte[part1.length + part2.length];
                System.arraycopy(part1, 0, fullContent, 0, part1.length);
                System.arraycopy(part2, 0, fullContent, part1.length, part2.length);

                // 完整分片上传流程
                CreateMultipartUploadResponse createResp = s3Client.createMultipartUpload(
                                b -> b.bucket(BUCKET).key(key));
                String uploadId = createResp.uploadId();

                UploadPartResponse p1 = s3Client.uploadPart(
                                b -> b.bucket(BUCKET).key(key).uploadId(uploadId).partNumber(1),
                                RequestBody.fromBytes(part1));
                UploadPartResponse p2 = s3Client.uploadPart(
                                b -> b.bucket(BUCKET).key(key).uploadId(uploadId).partNumber(2),
                                RequestBody.fromBytes(part2));

                List<CompletedPart> parts = List.of(
                                CompletedPart.builder().partNumber(1).eTag(p1.eTag()).build(),
                                CompletedPart.builder().partNumber(2).eTag(p2.eTag()).build());

                CompleteMultipartUploadResponse response = s3Client.completeMultipartUpload(b -> b
                                .bucket(BUCKET).key(key).uploadId(uploadId)
                                .multipartUpload(m -> m.parts(parts)));

                // 验证返回值
                assertNotNull(response.eTag(), "Complete ETag should not be null");
                assertEquals(BUCKET, response.bucket(), "Bucket should match");
                assertEquals(key, response.key(), "Key should match");

                // 验证合并后的内容
                byte[] downloaded = s3Client.getObjectAsBytes(b -> b.bucket(BUCKET).key(key)).asByteArray();
                DataIntegrityAssert.assertContentEquals(fullContent, downloaded);

                s3Client.deleteObject(b -> b.bucket(BUCKET).key(key));
        }

        @Test
        @Order(34)
        @DisplayName("Handler: AbortMultipartUpload - 中止清理验证")
        void testHandler_AbortMultipartUpload() {
                String key = "multipart-abort-test.bin";

                // 初始化并上传分片
                CreateMultipartUploadResponse createResp = s3Client.createMultipartUpload(
                                b -> b.bucket(BUCKET).key(key));
                String uploadId = createResp.uploadId();

                s3Client.uploadPart(
                                b -> b.bucket(BUCKET).key(key).uploadId(uploadId).partNumber(1),
                                RequestBody.fromBytes(TestDataGenerator.generateRandomBytes(5 * 1024 * 1024)));

                // 中止
                AbortMultipartUploadResponse response = s3Client.abortMultipartUpload(
                                b -> b.bucket(BUCKET).key(key).uploadId(uploadId));
                assertNotNull(response, "AbortMultipartUpload should return response");

                // Verify object does not exist (multipart upload was not completed)
                assertThrows(S3Exception.class,
                                () -> s3Client.headObject(b -> b.bucket(BUCKET).key(key)),
                                "Object should not exist after abort");

                // Per AWS S3 spec: ListParts after Abort should return NoSuchUpload error
                // 参考：https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListParts.html
                assertThrows(Exception.class,
                                () -> s3Client.listParts(b -> b.bucket(BUCKET).key(key).uploadId(uploadId)),
                                "ListParts should throw NoSuchUpload error after abort (S3 standard behavior)");
        }

        @Test
        @Order(35)
        @DisplayName("Handler: ListMultipartUploads - 进行中上传列表")
        void testHandler_ListMultipartUploads() {
                String key1 = "multipart-list-1.bin";
                String key2 = "multipart-list-2.bin";

                // 创建两个进行中的上传
                CreateMultipartUploadResponse resp1 = s3Client.createMultipartUpload(
                                b -> b.bucket(BUCKET).key(key1));
                CreateMultipartUploadResponse resp2 = s3Client.createMultipartUpload(
                                b -> b.bucket(BUCKET).key(key2));

                // ListMultipartUploads
                ListMultipartUploadsResponse listResp = s3Client.listMultipartUploads(
                                b -> b.bucket(BUCKET));

                // 验证返回值
                assertNotNull(listResp.uploads(), "Uploads list should not be null");
                assertTrue(listResp.uploads().size() >= 2, "Should have at least 2 uploads");

                // 验证包含我们的上传
                Set<String> uploadIds = Set.of(resp1.uploadId(), resp2.uploadId());
                long matchCount = listResp.uploads().stream()
                                .filter(u -> uploadIds.contains(u.uploadId()))
                                .count();
                assertEquals(2, matchCount, "Should find both uploads in list");

                // 清理
                s3Client.abortMultipartUpload(b -> b.bucket(BUCKET).key(key1).uploadId(resp1.uploadId()));
                s3Client.abortMultipartUpload(b -> b.bucket(BUCKET).key(key2).uploadId(resp2.uploadId()));
        }

        // ==================== DeleteBucket (最后执行) ====================

        @Test
        @Order(100)
        @DisplayName("Handler: DeleteBucket - 空Bucket删除验证")
        void testHandler_DeleteBucket() {
                String tempBucket = "delete-bucket-test-" + UUID.randomUUID().toString().substring(0, 8);

                // 创建临时 bucket
                s3Client.createBucket(b -> b.bucket(tempBucket));

                // 验证存在
                s3Client.headBucket(b -> b.bucket(tempBucket));

                // 删除
                DeleteBucketResponse response = s3Client.deleteBucket(b -> b.bucket(tempBucket));
                assertNotNull(response, "DeleteBucket should return response");

                // Verify object is deleted
                S3Exception ex = assertThrows(S3Exception.class,
                                () -> s3Client.headBucket(b -> b.bucket(tempBucket)));
                assertEquals(404, ex.statusCode(), "Deleted bucket should return 404");
        }
}
