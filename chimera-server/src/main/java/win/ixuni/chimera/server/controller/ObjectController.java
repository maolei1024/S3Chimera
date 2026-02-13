package win.ixuni.chimera.server.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities;
import win.ixuni.chimera.core.exception.CapabilityNotSupportedException;
import win.ixuni.chimera.core.model.AccessControlPolicy;
import win.ixuni.chimera.core.model.CopyObjectResult;
import win.ixuni.chimera.core.model.S3Object;
import win.ixuni.chimera.core.util.AwsChunkedDecoder;
import win.ixuni.chimera.core.util.S3ValidationUtils;
import win.ixuni.chimera.server.service.S3Service;

import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Object 操作控制器
 * <p>
 * 处理 S3 Object 相关的 API 请求，包括 Range 请求支持
 */
@Slf4j
@RestController
@RequiredArgsConstructor
public class ObjectController {

    private final S3Service s3Service;

    private static final DateTimeFormatter HTTP_DATE_FORMATTER = DateTimeFormatter
            .ofPattern("EEE, dd MMM yyyy HH:mm:ss 'GMT'", Locale.US)
            .withZone(ZoneOffset.UTC);

    /**
     * ISO 8601 date format (for CopyObject and other XML responses)
     */
    private static final DateTimeFormatter ISO_DATE_FORMATTER = DateTimeFormatter.ISO_INSTANT;

    private static final String APPLICATION_XML = "application/xml";

    /**
     * Range 请求格式: bytes=start-end 或 bytes=start- 或 bytes=-suffix
     */
    private static final Pattern RANGE_PATTERN = Pattern.compile("bytes=(\\d*)-(\\d*)");

    /**
     * 上传对象
     * PUT /{bucket}/{key}
     */
    @PutMapping(value = "/{bucket}/{*key}", produces = APPLICATION_XML)
    public Mono<ResponseEntity<?>> putObject(
            @PathVariable String bucket,
            @PathVariable String key,
            @RequestHeader(value = HttpHeaders.CONTENT_TYPE, required = false) String contentType,
            @RequestHeader Map<String, String> headers,
            @RequestHeader(value = "x-amz-copy-source", required = false) String copySource,
            @RequestHeader(value = "x-amz-content-sha256", required = false) String contentSha256,
            @RequestBody(required = false) Flux<DataBuffer> body) {

        // 如果是复制请求，交给 copyObject 处理
        if (copySource != null && !copySource.isEmpty()) {
            return copyObject(bucket, key, copySource);
        }

        // Extract user metadata
        Map<String, String> metadata = new HashMap<>();
        headers.forEach((k, v) -> {
            if (k.toLowerCase().startsWith("x-amz-meta-")) {
                metadata.put(k.substring(11), v);
            }
        });

        // 验证 Metadata (AWS S3 标准限制)
        String validationError = S3ValidationUtils.validateMetadata(metadata);
        if (validationError != null) {
            return Mono.just(ResponseEntity.badRequest()
                    .header("Content-Type", APPLICATION_XML)
                    .body("<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>InvalidArgument</Code><Message>"
                            + validationError + "</Message></Error>"));
        }

        // 处理空body的情况（如创建目录占位符或0字节文件）
        Flux<DataBuffer> safeBody = body != null ? body : Flux.empty();

        // 如果是 AWS Chunked Encoding，先解码
        Flux<ByteBuffer> content;
        if (AwsChunkedDecoder.isAwsChunkedEncoding(contentSha256)) {
            content = AwsChunkedDecoder.decode(safeBody);
        } else {
            // 普通上传，直接转换 DataBuffer 为 ByteBuffer
            content = safeBody.map(dataBuffer -> {
                byte[] bytes = new byte[dataBuffer.readableByteCount()];
                dataBuffer.read(bytes);
                DataBufferUtils.release(dataBuffer);
                return ByteBuffer.wrap(bytes);
            });
        }

        return s3Service.putObject(bucket, normalizeKey(key), content, contentType, metadata)
                .map(obj -> ResponseEntity.ok()
                        .header("ETag", "\"" + obj.getEtag() + "\"")
                        .<Void>build());
    }

    /**
     * 获取对象（支持 Range 请求）
     * GET /{bucket}/{key}
     */
    @GetMapping("/{bucket}/{*key}")
    public Mono<ResponseEntity<Flux<DataBuffer>>> getObject(
            @PathVariable String bucket,
            @PathVariable String key,
            @RequestHeader(value = HttpHeaders.RANGE, required = false) String rangeHeader) {

        String normalizedKey = normalizeKey(key);

        // If no Range request, return complete object
        if (rangeHeader == null || rangeHeader.isEmpty()) {
            return getFullObject(bucket, normalizedKey);
        }

        // 解析 Range 请求
        return parseAndProcessRange(bucket, normalizedKey, rangeHeader);
    }

    /**
     * 获取完整对象
     */
    private Mono<ResponseEntity<Flux<DataBuffer>>> getFullObject(String bucket, String key) {
        return s3Service.getObject(bucket, key)
                .map(objectData -> {
                    S3Object metadata = objectData.getMetadata();

                    Flux<DataBuffer> dataBufferFlux = objectData.getContent()
                            .map(byteBuffer -> new DefaultDataBufferFactory().wrap(byteBuffer));

                    ResponseEntity.BodyBuilder builder = ResponseEntity.ok()
                            .header(HttpHeaders.CONTENT_TYPE, metadata.getContentType())
                            .header(HttpHeaders.CONTENT_LENGTH, String.valueOf(metadata.getSize()))
                            .header("ETag", "\"" + metadata.getEtag() + "\"")
                            .header("Last-Modified", HTTP_DATE_FORMATTER.format(metadata.getLastModified()))
                            .header(HttpHeaders.ACCEPT_RANGES, "bytes");

                    return appendMetadataHeaders(builder, metadata.getUserMetadata())
                            .body(dataBufferFlux);
                })
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    /**
     * 解析并处理 Range 请求
     */
    private Mono<ResponseEntity<Flux<DataBuffer>>> parseAndProcessRange(
            String bucket, String key, String rangeHeader) {

        // First get object metadata to determine total size
        return s3Service.headObject(bucket, key)
                .flatMap(metadata -> {
                    long totalSize = metadata.getSize();
                    RangeSpec range = parseRange(rangeHeader, totalSize);

                    if (range == null) {
                        // 无效的 Range 请求
                        return Mono.just(ResponseEntity.status(HttpStatus.REQUESTED_RANGE_NOT_SATISFIABLE)
                                .header(HttpHeaders.CONTENT_RANGE, "bytes */" + totalSize)
                                .<Flux<DataBuffer>>build());
                    }

                    // 检查驱动是否支持 Range 请求
                    return s3Service.getDriverForBucket(bucket)
                            .flatMap(driver -> {
                                if (!driver.supports(DriverCapabilities.Capability.RANGE_READ)) {
                                    return Mono.error(new CapabilityNotSupportedException(
                                            DriverCapabilities.Capability.RANGE_READ, driver.getDriverName()));
                                }

                                // 执行 Range 读取
                                return s3Service.getObjectRange(bucket, key, range.start, range.end)
                                        .map(objectData -> {
                                            Flux<DataBuffer> dataBufferFlux = objectData.getContent()
                                                    .map(byteBuffer -> new DefaultDataBufferFactory().wrap(byteBuffer));

                                            long contentLength = range.end - range.start + 1;
                                            String contentRange = String.format("bytes %d-%d/%d",
                                                    range.start, range.end, totalSize);

                                            return ResponseEntity.status(HttpStatus.PARTIAL_CONTENT)
                                                    .header(HttpHeaders.CONTENT_TYPE, metadata.getContentType())
                                                    .header(HttpHeaders.CONTENT_LENGTH, String.valueOf(contentLength))
                                                    .header(HttpHeaders.CONTENT_RANGE, contentRange)
                                                    .header("ETag", "\"" + metadata.getEtag() + "\"")
                                                    .header("Last-Modified",
                                                            HTTP_DATE_FORMATTER.format(metadata.getLastModified()))
                                                    .header(HttpHeaders.ACCEPT_RANGES, "bytes")
                                                    .body(dataBufferFlux);
                                        });
                            });
                })
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    /**
     * 解析 Range 头
     *
     * @param rangeHeader Range 头值，如 "bytes=0-999" 或 "bytes=1000-" 或 "bytes=-500"
     * @param totalSize   文件总大小
     * @return 解析后的范围，null 表示无效
     */
    private RangeSpec parseRange(String rangeHeader, long totalSize) {
        if (!rangeHeader.startsWith("bytes=")) {
            return null;
        }

        Matcher matcher = RANGE_PATTERN.matcher(rangeHeader);
        if (!matcher.matches()) {
            return null;
        }

        String startStr = matcher.group(1);
        String endStr = matcher.group(2);

        long start, end;

        if (startStr.isEmpty() && !endStr.isEmpty()) {
            // bytes=-500 表示最后 500 字节
            long suffix = Long.parseLong(endStr);
            start = Math.max(0, totalSize - suffix);
            end = totalSize - 1;
        } else if (!startStr.isEmpty() && endStr.isEmpty()) {
            // bytes=1000- 表示从 1000 到末尾
            start = Long.parseLong(startStr);
            end = totalSize - 1;
        } else if (!startStr.isEmpty() && !endStr.isEmpty()) {
            // bytes=0-999
            start = Long.parseLong(startStr);
            end = Long.parseLong(endStr);
        } else {
            return null;
        }

        // Validate range validity
        if (start < 0 || start >= totalSize || end < start) {
            return null;
        }

        // 限制 end 不超过文件末尾
        end = Math.min(end, totalSize - 1);

        return new RangeSpec(start, end);
    }

    /**
     * Range 规范
     */
    private record RangeSpec(long start, long end) {
    }

    /**
     * 获取对象 ACL
     * GET /{bucket}/{key}?acl
     */
    @GetMapping(value = "/{bucket}/{*key}", params = "acl", produces = APPLICATION_XML)
    public Mono<ResponseEntity<AccessControlPolicy>> getObjectAcl(
            @PathVariable String bucket,
            @PathVariable String key) {

        return s3Service.headObject(bucket, normalizeKey(key))
                .map(obj -> {
                    AccessControlPolicy acl = AccessControlPolicy.createDefaultFullControl("chimera", "S3Chimera");
                    return ResponseEntity.ok(acl);
                })
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    /**
     * 获取对象元数据
     * HEAD /{bucket}/{key}
     */
    @RequestMapping(value = "/{bucket}/{*key}", method = RequestMethod.HEAD)
    public Mono<ResponseEntity<Void>> headObject(
            @PathVariable String bucket,
            @PathVariable String key) {

        return s3Service.headObject(bucket, normalizeKey(key))
                .map(obj -> {
                    ResponseEntity.BodyBuilder builder = ResponseEntity.ok()
                            .header(HttpHeaders.CONTENT_TYPE, obj.getContentType())
                            .header(HttpHeaders.CONTENT_LENGTH, String.valueOf(obj.getSize()))
                            .header("ETag", "\"" + obj.getEtag() + "\"")
                            .header("Last-Modified", HTTP_DATE_FORMATTER.format(obj.getLastModified()))
                            .header(HttpHeaders.ACCEPT_RANGES, "bytes");

                    return appendMetadataHeaders(builder, obj.getUserMetadata())
                            .<Void>build();
                })
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    /**
     * 删除对象
     * DELETE /{bucket}/{key}
     */
    @DeleteMapping("/{bucket}/{*key}")
    public Mono<ResponseEntity<Void>> deleteObject(
            @PathVariable String bucket,
            @PathVariable String key) {

        return s3Service.deleteObject(bucket, normalizeKey(key))
                .then(Mono.just(ResponseEntity.noContent().<Void>build()));
    }

    /**
     * 复制对象
     * PUT /{bucket}/{key} with x-amz-copy-source header
     *
     * Return CopyObjectResult XML response containing ETag and LastModified
     */
    private Mono<ResponseEntity<?>> copyObject(
            String bucket,
            String key,
            String copySource) {

        // 解析源 bucket/key，格式: /sourceBucket/sourceKey 或 sourceBucket/sourceKey
        String source = copySource.startsWith("/") ? copySource.substring(1) : copySource;
        int slashIndex = source.indexOf('/');
        if (slashIndex <= 0) {
            return Mono.just(ResponseEntity.badRequest().build());
        }

        // x-amz-copy-source header 中的值是 URL 编码的，需要解码
        String sourceBucket = URLDecoder.decode(source.substring(0, slashIndex), StandardCharsets.UTF_8);
        String sourceKey = URLDecoder.decode(source.substring(slashIndex + 1), StandardCharsets.UTF_8);

        log.debug("copyObject: sourceBucket='{}', sourceKey='{}' -> destBucket='{}', destKey='{}'",
                sourceBucket, sourceKey, bucket, normalizeKey(key));

        return s3Service.copyObject(sourceBucket, sourceKey, bucket, normalizeKey(key))
                .<ResponseEntity<?>>map(obj -> {
                    // Build CopyObjectResult XML response
                    CopyObjectResult result = CopyObjectResult.builder()
                            .etag("\"" + obj.getEtag() + "\"")
                            .lastModified(ISO_DATE_FORMATTER.format(obj.getLastModified()))
                            .build();

                    return ResponseEntity.ok()
                            .header("ETag", "\"" + obj.getEtag() + "\"")
                            .body(result);
                })
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    /**
     * Add user metadata headers
     */
    private ResponseEntity.BodyBuilder appendMetadataHeaders(ResponseEntity.BodyBuilder builder,
            Map<String, String> userMetadata) {
        if (userMetadata != null) {
            userMetadata.forEach((k, v) -> builder.header("x-amz-meta-" + k, v));
        }
        return builder;
    }

    /**
     * 规范化对象Key（去除开头的斜杠）
     */
    private String normalizeKey(String key) {
        if (key != null && key.startsWith("/")) {
            return key.substring(1);
        }
        return key;
    }
}
