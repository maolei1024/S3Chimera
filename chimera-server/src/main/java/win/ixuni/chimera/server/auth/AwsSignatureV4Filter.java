package win.ixuni.chimera.server.auth;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.auth.CredentialsProvider;
import win.ixuni.chimera.core.auth.S3Credentials;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * AWS Signature V4 authentication filter
 * <p>
 * 验证请求中的AWS Signature V4签名。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class AwsSignatureV4Filter implements WebFilter {

    private final AuthProperties authProperties;
    private final CredentialsProvider credentialsProvider;

    private static final String ALGORITHM = "AWS4-HMAC-SHA256";
    private static final String SERVICE = "s3";
    private static final Pattern AUTH_HEADER_PATTERN = Pattern.compile(
            "AWS4-HMAC-SHA256\\s+" +
                    "Credential=([^/]+)/([^/]+)/([^/]+)/([^/]+)/aws4_request,\\s*" +
                    "SignedHeaders=([^,]+),\\s*" +
                    "Signature=([a-f0-9]+)");

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        String path = exchange.getRequest().getURI().getPath();

        // Skip actuator endpoints (health checks, etc.)
        if (path.startsWith("/actuator")) {
            return chain.filter(exchange);
        }

        // If authentication is not enabled, pass through
        if (!authProperties.isEnabled()) {
            return chain.filter(exchange);
        }

        ServerHttpRequest request = exchange.getRequest();
        HttpMethod method = request.getMethod();

        // 检查是否允许匿名访问
        if (isAnonymousAllowed(method)) {
            String authHeader = request.getHeaders().getFirst(HttpHeaders.AUTHORIZATION);
            if (authHeader == null || authHeader.isEmpty()) {
                return chain.filter(exchange);
            }
        }

        // 验证签名
        return validateSignature(request)
                .flatMap(valid -> {
                    if (valid) {
                        return chain.filter(exchange);
                    } else {
                        return unauthorized(exchange, "SignatureDoesNotMatch",
                                "The request signature we calculated does not match the signature you provided.");
                    }
                })
                .onErrorResume(e -> {
                    log.error("Authentication error: {}", e.getMessage());
                    return unauthorized(exchange, "AccessDenied", e.getMessage());
                });
    }

    private boolean isAnonymousAllowed(HttpMethod method) {
        if (method == HttpMethod.GET || method == HttpMethod.HEAD) {
            return authProperties.isAllowAnonymousRead();
        }
        return authProperties.isAllowAnonymousWrite();
    }

    private Mono<Boolean> validateSignature(ServerHttpRequest request) {
        String authHeader = request.getHeaders().getFirst(HttpHeaders.AUTHORIZATION);
        if (authHeader == null || !authHeader.startsWith(ALGORITHM)) {
            return Mono.error(new SecurityException("Missing or invalid Authorization header"));
        }

        Matcher matcher = AUTH_HEADER_PATTERN.matcher(authHeader);
        if (!matcher.matches()) {
            return Mono.error(new SecurityException("Invalid Authorization header format"));
        }

        String accessKeyId = matcher.group(1);
        String dateStamp = matcher.group(2);
        String region = matcher.group(3);
        String service = matcher.group(4);
        String signedHeaders = matcher.group(5);
        String providedSignature = matcher.group(6);

        // 获取凭证并验证签名
        return credentialsProvider.getCredentials(accessKeyId)
                .switchIfEmpty(Mono.error(new SecurityException("Invalid access key")))
                .flatMap(credentials -> {
                    if (!credentials.isEnabled()) {
                        return Mono.error(new SecurityException("Access key is disabled"));
                    }

                    try {
                        String calculatedSignature = calculateSignature(
                                request, credentials, dateStamp, region, signedHeaders);
                        return Mono.just(calculatedSignature.equals(providedSignature));
                    } catch (Exception e) {
                        log.error("Error calculating signature: {}", e.getMessage());
                        return Mono.just(false);
                    }
                });
    }

    private String calculateSignature(ServerHttpRequest request, S3Credentials credentials,
            String dateStamp, String region, String signedHeadersStr) throws Exception {
        String secretKey = credentials.getSecretAccessKey();
        HttpMethod method = request.getMethod();
        URI uri = request.getURI();

        // 1. 创建规范请求
        String canonicalRequest = createCanonicalRequest(request, signedHeadersStr);

        // 2. 创建待签字符串
        String amzDate = request.getHeaders().getFirst("x-amz-date");
        if (amzDate == null) {
            amzDate = request.getHeaders().getFirst("X-Amz-Date");
        }
        String credentialScope = dateStamp + "/" + region + "/" + SERVICE + "/aws4_request";
        String stringToSign = ALGORITHM + "\n" +
                amzDate + "\n" +
                credentialScope + "\n" +
                sha256Hex(canonicalRequest);

        // 3. 计算签名密钥
        byte[] signingKey = getSignatureKey(secretKey, dateStamp, region, SERVICE);

        // 4. 计算签名
        return hmacSha256Hex(signingKey, stringToSign);
    }

    private String createCanonicalRequest(ServerHttpRequest request, String signedHeadersStr) throws Exception {
        HttpMethod method = request.getMethod();
        URI uri = request.getURI();
        HttpHeaders headers = request.getHeaders();

        // 规范URI - 按照 AWS S3 签名 V4 规范进行编码
        String canonicalUri = createCanonicalUri(uri.getRawPath());
        if (canonicalUri.isEmpty()) {
            canonicalUri = "/";
        }

        // 规范查询字符串
        String canonicalQueryString = createCanonicalQueryString(uri.getRawQuery());

        // 规范头部
        String[] signedHeaders = signedHeadersStr.split(";");
        Arrays.sort(signedHeaders);

        StringBuilder canonicalHeaders = new StringBuilder();
        for (String header : signedHeaders) {
            String value = headers.getFirst(header);
            if (value == null) {
                value = headers.getFirst(header.toLowerCase());
            }
            canonicalHeaders.append(header.toLowerCase()).append(":").append(value != null ? value.trim() : "")
                    .append("\n");
        }

        // Payload hash
        String payloadHash = headers.getFirst("x-amz-content-sha256");
        if (payloadHash == null) {
            payloadHash = "UNSIGNED-PAYLOAD";
        }

        return method.name() + "\n" +
                canonicalUri + "\n" +
                canonicalQueryString + "\n" +
                canonicalHeaders + "\n" +
                signedHeadersStr + "\n" +
                payloadHash;
    }

    private String createCanonicalQueryString(String rawQuery) {
        if (rawQuery == null || rawQuery.isEmpty()) {
            return "";
        }

        return Arrays.stream(rawQuery.split("&"))
                .map(param -> {
                    String[] parts = param.split("=", 2);
                    if (parts.length == 2) {
                        return parts[0] + "=" + parts[1];
                    }
                    return parts[0] + "=";
                })
                .sorted()
                .collect(Collectors.joining("&"));
    }

    /**
     * 创建规范化的 URI 路径
     * 按照 AWS S3 签名 V4 规范对路径进行编码
     */
    private String createCanonicalUri(String rawPath) {
        if (rawPath == null || rawPath.isEmpty()) {
            return "/";
        }

        // 分割路径段，对每个段进行规范化编码
        String[] segments = rawPath.split("/", -1);
        StringBuilder canonicalPath = new StringBuilder();

        for (int i = 0; i < segments.length; i++) {
            if (i > 0) {
                canonicalPath.append("/");
            }
            if (!segments[i].isEmpty()) {
                // First decode (handle potentially encoded characters), then re-encode per S3 spec
                String decoded = urlDecode(segments[i]);
                canonicalPath.append(s3UriEncode(decoded, false));
            }
        }

        String result = canonicalPath.toString();
        return result.isEmpty() ? "/" : result;
    }

    /**
     * URL 解码
     */
    private String urlDecode(String value) {
        try {
            return URLDecoder.decode(value, StandardCharsets.UTF_8.name());
        } catch (Exception e) {
            return value;
        }
    }

    /**
     * 按照 AWS S3 签名 V4 规范进行 URI 编码
     * Only A-Za-z0-9-._~ are not encoded; all other characters need encoding
     * 空格编码为 %20（不是 +）
     * 十六进制字母必须大写
     *
     * @param input       要编码的字符串
     * @param encodeSlash 是否编码斜杠
     */
    private String s3UriEncode(String input, boolean encodeSlash) {
        if (input == null || input.isEmpty()) {
            return "";
        }

        StringBuilder result = new StringBuilder();
        byte[] bytes = input.getBytes(StandardCharsets.UTF_8);

        for (byte b : bytes) {
            char c = (char) (b & 0xFF);
            if (isUnreservedCharacter(c) || (c == '/' && !encodeSlash)) {
                result.append(c);
            } else {
                result.append(String.format("%%" + "%02X", b));
            }
        }

        return result.toString();
    }

    /**
     * 检查字符是否是 AWS URI 编码规范中的非保留字符
     * 非保留字符: A-Z a-z 0-9 - . _ ~
     */
    private boolean isUnreservedCharacter(char c) {
        return (c >= 'A' && c <= 'Z') ||
                (c >= 'a' && c <= 'z') ||
                (c >= '0' && c <= '9') ||
                c == '-' || c == '.' || c == '_' || c == '~';
    }

    private byte[] getSignatureKey(String key, String dateStamp, String region, String service) throws Exception {
        byte[] kSecret = ("AWS4" + key).getBytes(StandardCharsets.UTF_8);
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

    private Mono<Void> unauthorized(ServerWebExchange exchange, String code, String message) {
        exchange.getResponse().setStatusCode(HttpStatus.FORBIDDEN);
        exchange.getResponse().getHeaders().add("Content-Type", "application/xml");
        String body = String.format(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                        "<Error><Code>%s</Code><Message>%s</Message></Error>",
                code, message);
        return exchange.getResponse().writeWith(
                Mono.just(exchange.getResponse().bufferFactory().wrap(body.getBytes(StandardCharsets.UTF_8))));
    }
}
