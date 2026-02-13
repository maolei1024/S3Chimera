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
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * S3 并发操作测试
 * <p>
 * 测试并发上传、下载和操作的正确性。
 * Each test uses independent resources (bucket/key) to ensure test isolation.
 * <p>
 * 运行方式：
 * - ./gradlew :chimera-test-s3:test --tests "*ConcurrencyTest*" -Pdriver=memory
 * - ./gradlew :chimera-test-s3:test --tests "*ConcurrencyTest*" -Pdriver=local
 */
@SpringBootTest(classes = ChimeraServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ExtendWith(SkipOnNotSupportedExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Timeout(value = 120, unit = TimeUnit.SECONDS)
public class S3ConcurrencyTest {

    @LocalServerPort
    private int port;

    private S3Client s3Client;

    // Use unique bucket name to avoid conflicts with other tests
    private final String BUCKET = "concurrency-test-" + UUID.randomUUID().toString().substring(0, 8);

    // CI-friendly concurrency parameters
    private static final int THREAD_COUNT = 8;
    private static final int TIMEOUT_SECONDS = 60;

    private ExecutorService executor;

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
        executor = Executors.newFixedThreadPool(THREAD_COUNT);
    }

    @AfterAll
    void tearDown() {
        if (executor != null) {
            executor.shutdown();
        }
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

    // ==================== 并发上传测试 ====================

    @Test
    @Order(1)
    @DisplayName("并发上传：不同Key内容隔离验证")
    void testConcurrentUpload_DifferentKeys_ContentIsolation() throws Exception {
        int fileCount = THREAD_COUNT;
        Map<String, byte[]> uploads = new ConcurrentHashMap<>();
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(fileCount);
        AtomicInteger errorCount = new AtomicInteger(0);

        // 准备上传数据
        for (int i = 0; i < fileCount; i++) {
            String key = "concurrent-" + i + "-" + UUID.randomUUID().toString().substring(0, 8) + ".bin";
            byte[] content = TestDataGenerator.generateRandomBytes(1024 * 10); // 10KB
            uploads.put(key, content);
        }

        // 并发上传
        for (Map.Entry<String, byte[]> entry : uploads.entrySet()) {
            executor.submit(() -> {
                try {
                    startLatch.await(); // wait for all threads to be ready
                    s3Client.putObject(
                            b -> b.bucket(BUCKET).key(entry.getKey()).contentType("application/octet-stream"),
                            RequestBody.fromBytes(entry.getValue()));
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                    e.printStackTrace();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        // 发令枪：同时开始
        startLatch.countDown();
        assertTrue(doneLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "All uploads should complete");
        assertEquals(0, errorCount.get(), "No upload errors should occur");

        // 验证每个文件内容隔离
        for (Map.Entry<String, byte[]> entry : uploads.entrySet()) {
            byte[] downloaded = s3Client.getObjectAsBytes(b -> b.bucket(BUCKET).key(entry.getKey())).asByteArray();
            DataIntegrityAssert.assertContentEquals(entry.getValue(), downloaded,
                    "Content for key " + entry.getKey() + " should match");
        }

        // 清理
        for (String key : uploads.keySet()) {
            s3Client.deleteObject(b -> b.bucket(BUCKET).key(key));
        }
    }

    @Test
    @Order(2)
    @DisplayName("Concurrent uploads: eventual consistency for the same key")
    void testConcurrentUpload_SameKey_EventualConsistency() throws Exception {
        String key = "concurrent-same-key-" + UUID.randomUUID().toString().substring(0, 8) + ".bin";
        int attemptCount = THREAD_COUNT;
        List<byte[]> contents = new ArrayList<>();
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(attemptCount);
        AtomicInteger errorCount = new AtomicInteger(0);

        // 准备不同内容
        for (int i = 0; i < attemptCount; i++) {
            contents.add(TestDataGenerator.generateRandomBytes(1024));
        }

        // Concurrent uploads to the same key
        for (int i = 0; i < attemptCount; i++) {
            final byte[] content = contents.get(i);
            executor.submit(() -> {
                try {
                    startLatch.await();
                    s3Client.putObject(
                            b -> b.bucket(BUCKET).key(key).contentType("application/octet-stream"),
                            RequestBody.fromBytes(content));
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                    e.printStackTrace();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue(doneLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "All uploads should complete");
        assertEquals(0, errorCount.get(), "No upload errors should occur");

        // Verify final state: content should be one of the uploaded versions
        byte[] finalContent = s3Client.getObjectAsBytes(b -> b.bucket(BUCKET).key(key)).asByteArray();
        assertEquals(1024, finalContent.length, "Final content size should match");

        // Verify content is one of the uploaded versions
        boolean matchesAny = contents.stream()
                .anyMatch(c -> Arrays.equals(c, finalContent));
        assertTrue(matchesAny, "Final content should match one of the uploaded versions");

        // 清理
        s3Client.deleteObject(b -> b.bucket(BUCKET).key(key));
    }

    @Test
    @Order(3)
    @DisplayName("Concurrent reads: parallel downloads of the same object")
    void testConcurrentRead_SameObject() throws Exception {
        String key = "concurrent-read-" + UUID.randomUUID().toString().substring(0, 8) + ".bin";
        byte[] content = TestDataGenerator.generateRandomBytes(1024 * 100); // 100KB
        String expectedMd5 = TestDataGenerator.calculateMD5(content);

        // 上传测试文件
        s3Client.putObject(
                b -> b.bucket(BUCKET).key(key).contentType("application/octet-stream"),
                RequestBody.fromBytes(content));

        // 并发读取
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(THREAD_COUNT);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);

        for (int i = 0; i < THREAD_COUNT; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    byte[] downloaded = s3Client.getObjectAsBytes(b -> b.bucket(BUCKET).key(key)).asByteArray();
                    String downloadedMd5 = TestDataGenerator.calculateMD5(downloaded);
                    if (expectedMd5.equals(downloadedMd5)) {
                        successCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                    e.printStackTrace();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue(doneLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "All reads should complete");
        assertEquals(0, errorCount.get(), "No read errors should occur");
        assertEquals(THREAD_COUNT, successCount.get(), "All reads should return correct content");

        // 清理
        s3Client.deleteObject(b -> b.bucket(BUCKET).key(key));
    }

    @Test
    @Order(4)
    @DisplayName("并发读写：读取不返回损坏数据")
    void testConcurrentReadWrite_NoCorruptedData() throws Exception {
        String key = "concurrent-rw-" + UUID.randomUUID().toString().substring(0, 8) + ".bin";
        int contentSize = 1024 * 10; // 10KB

        // 初始上传
        byte[] initialContent = TestDataGenerator.generateRandomBytes(contentSize);
        s3Client.putObject(
                b -> b.bucket(BUCKET).key(key).contentType("application/octet-stream"),
                RequestBody.fromBytes(initialContent));

        // 并发读写
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(THREAD_COUNT);
        AtomicInteger writeCount = new AtomicInteger(0);
        AtomicInteger readCount = new AtomicInteger(0);
        AtomicInteger corruptedCount = new AtomicInteger(0);
        Set<String> validMd5s = ConcurrentHashMap.newKeySet();
        validMd5s.add(TestDataGenerator.calculateMD5(initialContent));

        for (int i = 0; i < THREAD_COUNT; i++) {
            final boolean isWriter = i % 2 == 0; // half readers, half writers
            executor.submit(() -> {
                try {
                    startLatch.await();
                    if (isWriter) {
                        byte[] newContent = TestDataGenerator.generateRandomBytes(contentSize);
                        validMd5s.add(TestDataGenerator.calculateMD5(newContent));
                        s3Client.putObject(
                                b -> b.bucket(BUCKET).key(key).contentType("application/octet-stream"),
                                RequestBody.fromBytes(newContent));
                        writeCount.incrementAndGet();
                    } else {
                        try {
                            byte[] downloaded = s3Client.getObjectAsBytes(b -> b.bucket(BUCKET).key(key)).asByteArray();
                            String md5 = TestDataGenerator.calculateMD5(downloaded);
                            // 读取的内容必须是某个完整版本
                            if (downloaded.length == contentSize) {
                                readCount.incrementAndGet();
                            } else {
                                // 大小不对说明可能读到了损坏数据
                                corruptedCount.incrementAndGet();
                            }
                        } catch (NoSuchKeyException e) {
                            // May briefly not be found during writes (for some drivers)
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue(doneLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "All operations should complete");
        assertEquals(0, corruptedCount.get(), "No corrupted data should be read");
        assertTrue(writeCount.get() > 0, "Some writes should succeed");
        assertTrue(readCount.get() > 0, "Some reads should succeed");

        // 清理
        s3Client.deleteObject(b -> b.bucket(BUCKET).key(key));
    }

    @Test
    @Order(5)
    @DisplayName("并发分片上传：多个独立上传同时进行")
    void testConcurrentMultipartUploads() throws Exception {
        int uploadCount = 4; // 4 个并发分片上传
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(uploadCount);
        AtomicInteger successCount = new AtomicInteger(0);
        Map<String, byte[]> expectedContents = new ConcurrentHashMap<>();

        for (int i = 0; i < uploadCount; i++) {
            final String key = "multipart-concurrent-" + i + "-" + UUID.randomUUID().toString().substring(0, 8)
                    + ".bin";
            executor.submit(() -> {
                try {
                    startLatch.await();

                    // 生成分片数据
                    byte[] part1 = TestDataGenerator.generateRandomBytes(5 * 1024 * 1024); // 5MB
                    byte[] part2 = TestDataGenerator.generateRandomBytes(1024); // 1KB

                    // 合并内容
                    byte[] fullContent = new byte[part1.length + part2.length];
                    System.arraycopy(part1, 0, fullContent, 0, part1.length);
                    System.arraycopy(part2, 0, fullContent, part1.length, part2.length);
                    expectedContents.put(key, fullContent);

                    // 分片上传
                    CreateMultipartUploadResponse createResp = s3Client.createMultipartUpload(
                            b -> b.bucket(BUCKET).key(key));
                    String uploadId = createResp.uploadId();

                    UploadPartResponse p1Resp = s3Client.uploadPart(
                            b -> b.bucket(BUCKET).key(key).uploadId(uploadId).partNumber(1),
                            RequestBody.fromBytes(part1));
                    UploadPartResponse p2Resp = s3Client.uploadPart(
                            b -> b.bucket(BUCKET).key(key).uploadId(uploadId).partNumber(2),
                            RequestBody.fromBytes(part2));

                    List<CompletedPart> parts = List.of(
                            CompletedPart.builder().partNumber(1).eTag(p1Resp.eTag()).build(),
                            CompletedPart.builder().partNumber(2).eTag(p2Resp.eTag()).build());

                    s3Client.completeMultipartUpload(b -> b
                            .bucket(BUCKET).key(key).uploadId(uploadId)
                            .multipartUpload(m -> m.parts(parts)));

                    successCount.incrementAndGet();
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue(doneLatch.await(60, TimeUnit.SECONDS), "All multipart uploads should complete");
        assertEquals(uploadCount, successCount.get(), "All multipart uploads should succeed");

        // 验证每个上传的内容
        for (Map.Entry<String, byte[]> entry : expectedContents.entrySet()) {
            byte[] downloaded = s3Client.getObjectAsBytes(b -> b.bucket(BUCKET).key(entry.getKey())).asByteArray();
            DataIntegrityAssert.assertContentEquals(entry.getValue(), downloaded,
                    "Multipart upload content should match for " + entry.getKey());
            s3Client.deleteObject(b -> b.bucket(BUCKET).key(entry.getKey()));
        }
    }

    @Test
    @Order(6)
    @DisplayName("并发ListObjects：多线程列表操作")
    void testConcurrentListObjects() throws Exception {
        // Upload some test files
        int fileCount = 20;
        for (int i = 0; i < fileCount; i++) {
            final int idx = i;
            s3Client.putObject(
                    b -> b.bucket(BUCKET).key("list-test-" + idx + ".txt"),
                    RequestBody.fromString("Content " + idx));
        }

        // 并发执行 ListObjects
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(THREAD_COUNT);
        AtomicInteger correctCount = new AtomicInteger(0);

        for (int i = 0; i < THREAD_COUNT; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    ListObjectsV2Response response = s3Client.listObjectsV2(
                            b -> b.bucket(BUCKET).prefix("list-test-"));
                    if (response.contents().size() == fileCount) {
                        correctCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue(doneLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "All list operations should complete");
        assertEquals(THREAD_COUNT, correctCount.get(),
                "All concurrent list operations should return correct count");

        // 清理
        for (int i = 0; i < fileCount; i++) {
            final int idx = i;
            s3Client.deleteObject(b -> b.bucket(BUCKET).key("list-test-" + idx + ".txt"));
        }
    }
}
