package win.ixuni.chimera.server.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.auth.CredentialsProvider;
import win.ixuni.chimera.core.auth.S3Credentials;
import win.ixuni.chimera.server.auth.AuthProperties;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Pre-signed URL service
 * <p>
 * Supports generating and validating AWS Signature V4 pre-signed URLs
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class PresignedUrlService {

    private final CredentialsProvider credentialsProvider;
    private final AuthProperties authProperties;

    private static final String ALGORITHM = "AWS4-HMAC-SHA256";
    private static final String SERVICE = "s3";
    private static final String UNSIGNED_PAYLOAD = "UNSIGNED-PAYLOAD";

    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter
            .ofPattern("yyyyMMdd")
            .withZone(ZoneOffset.UTC);

    private static final DateTimeFormatter DATE_TIME_FORMAT = DateTimeFormatter
            .ofPattern("yyyyMMdd'T'HHmmss'Z'")
            .withZone(ZoneOffset.UTC);

    /**
     * Generate pre-signed download URL (GET)
     *
     * @param accessKeyId 访问密钥 ID
     * @param bucket      Bucket 名称
     * @param key         对象 Key
     * @param expiresIn expiration period (seconds)
     * @param region region
     * @param endpoint    服务端点 URL
     * @return pre-signed URL
     */
    public Mono<String> generatePresignedGetUrl(String accessKeyId, String bucket, String key,
                                                 int expiresIn, String region, String endpoint) {
        return generatePresignedUrl("GET", accessKeyId, bucket, key, expiresIn, region, endpoint, null);
    }

    /**
     * Generate pre-signed upload URL (PUT)
     *
     * @param accessKeyId 访问密钥 ID
     * @param bucket      Bucket 名称
     * @param key         对象 Key
     * @param expiresIn expiration period (seconds)
     * @param region region
     * @param endpoint    服务端点 URL
     * @param contentType 内容类型（可选）
     * @return pre-signed URL
     */
    public Mono<String> generatePresignedPutUrl(String accessKeyId, String bucket, String key,
                                                 int expiresIn, String region, String endpoint,
                                                 String contentType) {
        Map<String, String> additionalHeaders = new HashMap<>();
        if (contentType != null) {
            additionalHeaders.put("content-type", contentType);
        }
        return generatePresignedUrl("PUT", accessKeyId, bucket, key, expiresIn, region, endpoint, additionalHeaders);
    }

    /**
     * Generate pre-signed URL
     */
    private Mono<String> generatePresignedUrl(String method, String accessKeyId, String bucket, String key,
                                               int expiresIn, String region, String endpoint,
                                               Map<String, String> additionalHeaders) {
        return credentialsProvider.getCredentials(accessKeyId)
                .switchIfEmpty(Mono.error(new IllegalArgumentException("Invalid access key: " + accessKeyId)))
                .map(credentials -> {
                    try {
                        return buildPresignedUrl(method, credentials, bucket, key, expiresIn, region, endpoint, additionalHeaders);
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to generate presigned URL", e);
                    }
                });
    }

    /**
     * Verify pre-signed URL request
     *
     * @param method          HTTP 方法
     * @param uri             请求 URI（不含查询字符串）
     * @param queryParams     查询参数
     * @param headers         请求头
     * @return 验证结果
     */
    public Mono<Boolean> validatePresignedUrl(String method, String uri,
                                               Map<String, String> queryParams,
                                               Map<String, String> headers) {
        // Extract pre-signed URL parameters
        String algorithm = queryParams.get("X-Amz-Algorithm");
        String credential = queryParams.get("X-Amz-Credential");
        String date = queryParams.get("X-Amz-Date");
        String expires = queryParams.get("X-Amz-Expires");
        String signedHeaders = queryParams.get("X-Amz-SignedHeaders");
        String signature = queryParams.get("X-Amz-Signature");

        // 检查必要参数
        if (algorithm == null || credential == null || date == null ||
                expires == null || signedHeaders == null || signature == null) {
            return Mono.just(false);
        }

        // 检查是否过期
        try {
            Instant requestTime = Instant.from(DATE_TIME_FORMAT.parse(date));
            int expiresSeconds = Integer.parseInt(expires);
            Instant expiryTime = requestTime.plusSeconds(expiresSeconds);

            if (Instant.now().isAfter(expiryTime)) {
                log.debug("Presigned URL expired");
                return Mono.just(false);
            }
        } catch (Exception e) {
            log.error("Failed to parse presigned URL date: {}", e.getMessage());
            return Mono.just(false);
        }

        // 解析 Credential
        String[] credParts = credential.split("/");
        if (credParts.length < 5) {
            return Mono.just(false);
        }
        String accessKeyId = credParts[0];
        String dateStamp = credParts[1];
        String region = credParts[2];
        String service = credParts[3];

        // 获取凭证并验证签名
        return credentialsProvider.getCredentials(accessKeyId)
                .map(credentials -> {
                    try {
                        String calculatedSignature = calculatePresignedSignature(
                                method, uri, queryParams, headers,
                                credentials, dateStamp, region, date, signedHeaders
                        );
                        return calculatedSignature.equals(signature);
                    } catch (Exception e) {
                        log.error("Failed to validate presigned URL: {}", e.getMessage());
                        return false;
                    }
                })
                .defaultIfEmpty(false);
    }

    /**
     * Build pre-signed URL
     */
    private String buildPresignedUrl(String method, S3Credentials credentials,
                                      String bucket, String key, int expiresIn,
                                      String region, String endpoint,
                                      Map<String, String> additionalHeaders) throws Exception {
        Instant now = Instant.now();
        String dateStamp = DATE_FORMAT.format(now);
        String amzDate = DATE_TIME_FORMAT.format(now);

        String host = extractHost(endpoint);
        String canonicalUri = "/" + bucket + "/" + urlEncode(key, false);

        // 构建 Credential Scope
        String credentialScope = dateStamp + "/" + region + "/" + SERVICE + "/aws4_request";
        String credential = credentials.getAccessKeyId() + "/" + credentialScope;

        // 构建签名头
        String signedHeaders = "host";
        if (additionalHeaders != null && !additionalHeaders.isEmpty()) {
            signedHeaders = "host;" + String.join(";", additionalHeaders.keySet().stream().sorted().toList());
        }

        // 构建查询参数（按字母顺序）
        TreeMap<String, String> queryParams = new TreeMap<>();
        queryParams.put("X-Amz-Algorithm", ALGORITHM);
        queryParams.put("X-Amz-Credential", credential);
        queryParams.put("X-Amz-Date", amzDate);
        queryParams.put("X-Amz-Expires", String.valueOf(expiresIn));
        queryParams.put("X-Amz-SignedHeaders", signedHeaders);

        // 构建规范查询字符串
        StringBuilder canonicalQueryString = new StringBuilder();
        for (Map.Entry<String, String> entry : queryParams.entrySet()) {
            if (canonicalQueryString.length() > 0) {
                canonicalQueryString.append("&");
            }
            canonicalQueryString.append(urlEncode(entry.getKey(), true))
                    .append("=")
                    .append(urlEncode(entry.getValue(), true));
        }

        // 构建规范头
        StringBuilder canonicalHeaders = new StringBuilder();
        canonicalHeaders.append("host:").append(host).append("\n");
        if (additionalHeaders != null) {
            for (Map.Entry<String, String> entry : new TreeMap<>(additionalHeaders).entrySet()) {
                canonicalHeaders.append(entry.getKey().toLowerCase()).append(":").append(entry.getValue().trim()).append("\n");
            }
        }

        // 构建规范请求
        String canonicalRequest = method + "\n" +
                canonicalUri + "\n" +
                canonicalQueryString + "\n" +
                canonicalHeaders + "\n" +
                signedHeaders + "\n" +
                UNSIGNED_PAYLOAD;

        // 构建待签字符串
        String stringToSign = ALGORITHM + "\n" +
                amzDate + "\n" +
                credentialScope + "\n" +
                sha256Hex(canonicalRequest);

        // 计算签名
        byte[] signingKey = getSignatureKey(credentials.getSecretAccessKey(), dateStamp, region, SERVICE);
        String signature = hmacSha256Hex(signingKey, stringToSign);

        // 构建最终 URL
        return endpoint + canonicalUri + "?" + canonicalQueryString + "&X-Amz-Signature=" + signature;
    }

    /**
     * Compute signature for pre-signed request
     */
    private String calculatePresignedSignature(String method, String uri,
                                                Map<String, String> queryParams,
                                                Map<String, String> headers,
                                                S3Credentials credentials,
                                                String dateStamp, String region,
                                                String amzDate, String signedHeadersStr) throws Exception {
        // 构建规范查询字符串（不包含 X-Amz-Signature）
        TreeMap<String, String> sortedParams = new TreeMap<>(queryParams);
        sortedParams.remove("X-Amz-Signature");

        StringBuilder canonicalQueryString = new StringBuilder();
        for (Map.Entry<String, String> entry : sortedParams.entrySet()) {
            if (canonicalQueryString.length() > 0) {
                canonicalQueryString.append("&");
            }
            canonicalQueryString.append(urlEncode(entry.getKey(), true))
                    .append("=")
                    .append(urlEncode(entry.getValue(), true));
        }

        // 构建规范头
        String[] signedHeaders = signedHeadersStr.split(";");
        Arrays.sort(signedHeaders);

        StringBuilder canonicalHeaders = new StringBuilder();
        for (String header : signedHeaders) {
            String value = headers.get(header);
            if (value == null) {
                value = headers.get(header.toLowerCase());
            }
            canonicalHeaders.append(header.toLowerCase()).append(":").append(value != null ? value.trim() : "").append("\n");
        }

        // 构建规范请求
        String canonicalRequest = method + "\n" +
                uri + "\n" +
                canonicalQueryString + "\n" +
                canonicalHeaders + "\n" +
                signedHeadersStr + "\n" +
                UNSIGNED_PAYLOAD;

        // 构建待签字符串
        String credentialScope = dateStamp + "/" + region + "/" + SERVICE + "/aws4_request";
        String stringToSign = ALGORITHM + "\n" +
                amzDate + "\n" +
                credentialScope + "\n" +
                sha256Hex(canonicalRequest);

        // 计算签名
        byte[] signingKey = getSignatureKey(credentials.getSecretAccessKey(), dateStamp, region, SERVICE);
        return hmacSha256Hex(signingKey, stringToSign);
    }

    // ==================== 工具方法 ====================

    private String extractHost(String endpoint) {
        String host = endpoint.replaceFirst("https?://", "");
        int slashIndex = host.indexOf('/');
        if (slashIndex > 0) {
            host = host.substring(0, slashIndex);
        }
        return host;
    }

    private String urlEncode(String value, boolean encodeSlash) {
        try {
            String encoded = URLEncoder.encode(value, StandardCharsets.UTF_8)
                    .replace("+", "%20")
                    .replace("*", "%2A")
                    .replace("%7E", "~");
            if (!encodeSlash) {
                encoded = encoded.replace("%2F", "/");
            }
            return encoded;
        } catch (Exception e) {
            return value;
        }
    }

    private byte[] getSignatureKey(String secretKey, String dateStamp, String region, String service) throws Exception {
        byte[] kSecret = ("AWS4" + secretKey).getBytes(StandardCharsets.UTF_8);
        byte[] kDate = hmacSha256(kSecret, dateStamp);
        byte[] kRegion = hmacSha256(kDate, region);
        byte[] kService = hmacSha256(kRegion, service);
        return hmacSha256(kService, "aws4_request");
    }

    private byte[] hmacSha256(byte[] key, String data) throws Exception {
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(key, "HmacSHA256"));
        return mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
    }

    private String hmacSha256Hex(byte[] key, String data) throws Exception {
        return bytesToHex(hmacSha256(key, data));
    }

    private String sha256Hex(String data) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] hash = digest.digest(data.getBytes(StandardCharsets.UTF_8));
        return bytesToHex(hash);
    }

    private String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}
