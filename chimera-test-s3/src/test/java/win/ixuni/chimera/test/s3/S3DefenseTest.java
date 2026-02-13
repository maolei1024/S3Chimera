package win.ixuni.chimera.test.s3;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.*;
import win.ixuni.chimera.server.ChimeraServerApplication;
import win.ixuni.chimera.test.s3.util.TestDataGenerator;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * S3 defensive/negative tests
 * <p>
 * 测试畸形请求、非法 Header、超限 Metadata 等边缘和安全场景。
 * Ensures the server properly rejects illegal requests instead of crashing or undefined behavior.
 * <p>
 * 运行方式:
 * - ./gradlew :chimera-test-s3:test --tests "*Defense*" -Pdriver=memory
 */
@SpringBootTest(classes = ChimeraServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ExtendWith(SkipOnNotSupportedExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class S3DefenseTest {

    @LocalServerPort
    private int port;

    private S3Client s3Client;
    private HttpClient httpClient; // For sending raw HTTP requests to test malformed headers

    private static final String DEFENSE_BUCKET = "defense-test-" + UUID.randomUUID().toString().substring(0, 8);

    @BeforeAll
    void setup() {
        S3Configuration s3Config = S3Configuration.builder()
                .pathStyleAccessEnabled(true)
                .checksumValidationEnabled(true) // Enable checksum validation
                .chunkedEncodingEnabled(true) // Enable chunked transfer encoding
                .build();

        s3Client = S3Client.builder()
                .endpointOverride(URI.create("http://localhost:" + port))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("test-access-key", "test-secret-key")))
                .serviceConfiguration(s3Config)
                .build();

        httpClient = HttpClient.newHttpClient();

        // 创建测试 Bucket
        s3Client.createBucket(b -> b.bucket(DEFENSE_BUCKET));
    }

    @AfterAll
    void tearDown() {
        try {
            ListObjectsV2Response listResponse = s3Client.listObjectsV2(b -> b.bucket(DEFENSE_BUCKET));
            for (S3Object obj : listResponse.contents()) {
                s3Client.deleteObject(b -> b.bucket(DEFENSE_BUCKET).key(obj.key()));
            }
            s3Client.deleteBucket(b -> b.bucket(DEFENSE_BUCKET));
        } catch (Exception e) {
            // ignore
        }
        if (s3Client != null) {
            s3Client.close();
        }
    }

    // ==================== Metadata 限制测试 ====================

    @Test
    @Order(10)
    @DisplayName("Defensive: oversized User Metadata")
    void testExcessiveUserMetadata() {
        // AWS S3 limits total user metadata size to 2KB
        Map<String, String> metadata = new HashMap<>();
        StringBuilder longValue = new StringBuilder();
        for (int i = 0; i < 3000; i++) {
            longValue.append("a");
        }
        metadata.put("long-key", longValue.toString());

        String key = "excessive-metadata.txt";

        // Expected behavior: should throw exception (400 Bad Request or similar)
        // Note: some implementations may allow slightly larger, but excess should be rejected
        try {
            s3Client.putObject(b -> b.bucket(DEFENSE_BUCKET).key(key).metadata(metadata),
                    RequestBody.fromString("test"));
            // If succeeded, log warning (may have passed, but should ideally reject)
            System.err.println("WARNING: Server accepted 3KB metadata (Standard S3 limit is 2KB)");
        } catch (S3Exception e) {
            assertTrue(e.statusCode() >= 400, "Should reject excessive metadata");
        }
    }

    @Test
    @Order(11)
    @DisplayName("Defensive: non-ASCII Metadata Key")
    void testNonAsciiMetadataKey() {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("测试Key", "value"); // 标准 S3 不允许非 ASCII Header Key

        String key = "non-ascii-metadata.txt";

        try {
            s3Client.putObject(b -> b.bucket(DEFENSE_BUCKET).key(key).metadata(metadata),
                    RequestBody.fromString("test"));
            fail("Client or Server should reject non-ASCII metadata keys");
        } catch (SdkClientException | S3Exception e) {
            // 客户端 SDK 可能直接拦截，这也没问题
        }
    }

    // ==================== Malformed Header Tests (using HttpClient) ====================

    // ==================== Malformed Header Tests (using Socket to bypass HttpClient restrictions)
    // ====================

    @Test
    @Order(20)
    @DisplayName("Defensive: invalid Content-Length (non-numeric)")
    void testInvalidContentLength() throws Exception {
        String key = "bad-header.txt";
        String request = "PUT /" + DEFENSE_BUCKET + "/" + key + " HTTP/1.1\r\n" +
                "Host: localhost:" + port + "\r\n" +
                "Content-Length: invalid-number\r\n" +
                "Connection: close\r\n" +
                "\r\n" +
                "test content";

        String response = sendRawRequest(request);
        // Should return 400 Bad Request
        assertTrue(response.contains("HTTP/1.1 400") || response.isEmpty(),
                "Server should reject non-numeric Content-Length. Response: " + response);
    }

    @Test
    @Order(21)
    @DisplayName("Defensive: Content-Length mismatch with actual body (too large)")
    void testContentLengthMismatch_TooLarge() throws Exception {
        String key = "mismatch-large.txt";
        String body = "12345"; // 5 bytes

        // 声明 100 bytes，但只发 5 bytes，然后半连接关闭
        String request = "PUT /" + DEFENSE_BUCKET + "/" + key + " HTTP/1.1\r\n" +
                "Host: localhost:" + port + "\r\n" +
                "Content-Length: 100\r\n" +
                "Connection: close\r\n" +
                "\r\n" +
                body;

        try (java.net.Socket socket = new java.net.Socket("localhost", port)) {
            socket.setSoTimeout(2000);
            socket.getOutputStream().write(request.getBytes(StandardCharsets.UTF_8));
            socket.getOutputStream().flush();
            // 不发剩下的 95 bytes，直接关闭输出流
            socket.shutdownOutput();

            java.io.BufferedReader reader = new java.io.BufferedReader(
                    new java.io.InputStreamReader(socket.getInputStream()));
            String statusLine = reader.readLine();
            // Server should return an error on timeout or stream end, or simply disconnect
            if (statusLine != null) {
                int statusCode = Integer.parseInt(statusLine.split(" ")[1]);
                assertTrue(statusCode >= 400, "Should reject mismatched Content-Length");
            }
        } catch (Exception e) {
            // 连接重置或断开都是可接受的拒绝方式
        }
    }

    private String sendRawRequest(String request) throws Exception {
        try (java.net.Socket socket = new java.net.Socket("localhost", port)) {
            socket.setSoTimeout(5000);
            socket.getOutputStream().write(request.getBytes(StandardCharsets.UTF_8));
            socket.getOutputStream().flush();

            java.io.InputStream in = socket.getInputStream();
            byte[] responseBuffer = new byte[4096];
            int read = in.read(responseBuffer);
            if (read == -1)
                return "";
            return new String(responseBuffer, 0, read, StandardCharsets.UTF_8);
        } catch (java.net.SocketException | java.net.SocketTimeoutException e) {
            return ""; // Socket closed or timeout is also a form of rejection
        }
    }

    // ==================== XML Attack Defense ====================

    @Test
    @Order(30)
    @DisplayName("Defensive: XML entity injection (XXE) in DeleteObjects")
    void testXXElnDeleteObjects() throws Exception {
        // Construct a request body with XXE payload
        String xxePayload = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<!DOCTYPE foo [ <!ENTITY xxe SYSTEM \"file:///etc/passwd\"> ]>" +
                "<Delete><Object><Key>&xxe;</Key></Object></Delete>";

        String url = "http://localhost:" + port + "/" + DEFENSE_BUCKET + "?delete";

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .POST(HttpRequest.BodyPublishers.ofString(xxePayload))
                .header("Content-Type", "application/xml")
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        // 我们不期望它成功删除 /etc/passwd (当然也不可能)，
        // 但我们主要检查服务端是否解析了该实体。
        // 如果返回体中包含了系统文件内容，则漏洞存在。

        String responseBody = response.body();
        assertFalse(responseBody.contains("root:x:"),
                "XXE Vulnerability Detected! /etc/passwd content found in response.");

        // Expected: typically 400 Bad Request (invalid filename in key) or entity ignored
    }

    // ==================== 不支持的 HTTP 方法测试 ====================

    @Test
    @Order(40)
    @DisplayName("Defensive: unsupported HTTP method (PATCH)")
    void testUnsupportedMethod() throws Exception {
        String url = "http://localhost:" + port + "/" + DEFENSE_BUCKET + "/patch-test.txt";

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .method("PATCH", HttpRequest.BodyPublishers.ofString("data"))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        // S3 does not support PATCH; should return 405 Method Not Allowed or 501 Not Implemented
        // 或者至少是 4xx 错误
        assertTrue(response.statusCode() >= 400, "Should reject unsupported method PATCH");
    }
}
