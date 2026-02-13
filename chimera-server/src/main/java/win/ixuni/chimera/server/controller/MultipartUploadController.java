package win.ixuni.chimera.server.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.model.*;
import win.ixuni.chimera.core.util.AwsChunkedDecoder;
import win.ixuni.chimera.core.util.S3ValidationUtils;
import win.ixuni.chimera.server.service.S3Service;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * S3 分片上传控制器
 */
@Slf4j
@RestController
@RequiredArgsConstructor
public class MultipartUploadController {

        private final S3Service s3Service;

        /**
         * 创建分片上传
         * POST /{bucket}/{key}?uploads
         */
        @PostMapping(value = "/{bucket}/{*key}", params = "uploads")
        public Mono<ResponseEntity<InitiateMultipartUploadResult>> createMultipartUpload(
                        @PathVariable String bucket,
                        @PathVariable String key,
                        @RequestHeader(value = "Content-Type", required = false) String contentType,
                        @RequestHeader Map<String, String> headers) {

                log.info("CreateMultipartUpload request: bucket={}, key={}", bucket, key);

                String normalizedKey = normalizeKey(key);
                if (normalizedKey.isEmpty()) {
                        log.error("CreateMultipartUpload failed: key is empty after normalization, original key={}",
                                        key);
                        return Mono.just(ResponseEntity.badRequest().build());
                }

                // Extract user metadata
                Map<String, String> metadata = headers.entrySet().stream()
                                .filter(e -> e.getKey().toLowerCase().startsWith("x-amz-meta-"))
                                .collect(Collectors.toMap(
                                                e -> e.getKey().substring(11),
                                                Map.Entry::getValue));

                // 验证 Metadata (AWS S3 标准限制)
                String validationError = S3ValidationUtils.validateMetadata(metadata);
                if (validationError != null) {
                        return Mono.just(ResponseEntity.badRequest()
                                        .header("Content-Type", "application/xml")
                                        .body(null)); // S3Client will handle the error body or we can provide it later
                }

                return s3Service.createMultipartUpload(bucket, normalizedKey, contentType, metadata)
                                .map(upload -> {
                                        InitiateMultipartUploadResult result = new InitiateMultipartUploadResult();
                                        result.setBucket(bucket);
                                        result.setKey(normalizedKey);
                                        result.setUploadId(upload.getUploadId());
                                        log.debug("CreateMultipartUpload success: bucket={}, key={}, uploadId={}",
                                                        bucket, normalizedKey, upload.getUploadId());
                                        return ResponseEntity.ok(result);
                                });
        }

        /**
         * 上传分片
         * PUT /{bucket}/{key}?partNumber={partNumber}&uploadId={uploadId}
         */
        @PutMapping(value = "/{bucket}/{*key}", params = { "partNumber", "uploadId" })
        public Mono<ResponseEntity<Void>> uploadPart(
                        @PathVariable String bucket,
                        @PathVariable String key,
                        @RequestParam Integer partNumber,
                        @RequestParam String uploadId,
                        @RequestHeader(value = "x-amz-content-sha256", required = false) String contentSha256,
                        @RequestBody Flux<DataBuffer> body) {

                // 如果是 AWS Chunked Encoding，先解码
                Flux<ByteBuffer> content;
                if (AwsChunkedDecoder.isAwsChunkedEncoding(contentSha256)) {
                        content = AwsChunkedDecoder.decode(body);
                } else {
                        // 普通上传，直接转换 DataBuffer 为 ByteBuffer
                        content = body.map(dataBuffer -> {
                                ByteBuffer byteBuffer = dataBuffer.toByteBuffer();
                                ByteBuffer copy = ByteBuffer.allocate(byteBuffer.remaining());
                                copy.put(byteBuffer);
                                copy.flip();
                                DataBufferUtils.release(dataBuffer);
                                return copy;
                        });
                }

                return s3Service.uploadPart(bucket, normalizeKey(key), uploadId, partNumber, content)
                                .map(part -> ResponseEntity.ok()
                                                .header("ETag", "\"" + part.getEtag() + "\"")
                                                .<Void>build());
        }

        /**
         * 完成分片上传
         * POST /{bucket}/{key}?uploadId={uploadId}
         */
        @PostMapping(value = "/{bucket}/{*key}", params = "uploadId")
        public Mono<ResponseEntity<CompleteMultipartUploadResult>> completeMultipartUpload(
                        @PathVariable String bucket,
                        @PathVariable String key,
                        @RequestParam String uploadId,
                        @RequestBody CompleteMultipartUploadRequest request) {

                List<CompletedPart> parts = request.getParts().stream()
                                .map(p -> CompletedPart.builder()
                                                .partNumber(p.getPartNumber())
                                                .etag(p.getEtag().replace("\"", ""))
                                                .build())
                                .collect(Collectors.toList());

                return s3Service.completeMultipartUpload(bucket, normalizeKey(key), uploadId, parts)
                                .map(obj -> {
                                        CompleteMultipartUploadResult result = new CompleteMultipartUploadResult();
                                        result.setBucket(bucket);
                                        result.setKey(normalizeKey(key));
                                        result.setEtag(obj.getEtag());
                                        result.setLocation("/" + bucket + "/" + normalizeKey(key));
                                        return ResponseEntity.ok(result);
                                });
        }

        /**
         * 取消分片上传
         * DELETE /{bucket}/{key}?uploadId={uploadId}
         */
        @DeleteMapping(value = "/{bucket}/{*key}", params = "uploadId")
        public Mono<ResponseEntity<Void>> abortMultipartUpload(
                        @PathVariable String bucket,
                        @PathVariable String key,
                        @RequestParam String uploadId) {

                return s3Service.abortMultipartUpload(bucket, normalizeKey(key), uploadId)
                                .then(Mono.just(ResponseEntity.noContent().<Void>build()));
        }

        /**
         * List uploaded parts
         * GET /{bucket}/{key}?uploadId={uploadId}
         */
        @GetMapping(value = "/{bucket}/{*key}", params = "uploadId")
        public Mono<ResponseEntity<ListPartsResult>> listParts(
                        @PathVariable String bucket,
                        @PathVariable String key,
                        @RequestParam String uploadId,
                        @RequestParam(name = "part-number-marker", required = false) Integer partNumberMarker,
                        @RequestParam(name = "max-parts", required = false, defaultValue = "1000") Integer maxParts) {

                ListPartsRequest request = ListPartsRequest.builder()
                                .bucketName(bucket)
                                .key(normalizeKey(key))
                                .uploadId(uploadId)
                                .partNumberMarker(partNumberMarker)
                                .maxParts(maxParts)
                                .build();

                return s3Service.listParts(request)
                                .map(ResponseEntity::ok);
        }

        /**
         * List in-progress multipart uploads
         * GET /{bucket}?uploads
         */
        @GetMapping(value = "/{bucket}", params = "uploads")
        public Mono<ResponseEntity<ListMultipartUploadsResult>> listMultipartUploads(
                        @PathVariable String bucket,
                        @RequestParam(required = false) String prefix) {

                return s3Service.listMultipartUploads(bucket, prefix)
                                .map(ResponseEntity::ok);
        }

        private String normalizeKey(String key) {
                if (key == null || key.isEmpty()) {
                        log.warn("Received null or empty key in multipart upload request");
                        return "";
                }
                // Spring's {*key} captured path starts with /
                if (key.startsWith("/")) {
                        return key.substring(1);
                }
                return key;
        }
}
