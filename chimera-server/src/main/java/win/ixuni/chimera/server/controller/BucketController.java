package win.ixuni.chimera.server.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.config.ChimeraProperties;
import win.ixuni.chimera.core.model.*;
import win.ixuni.chimera.server.service.S3Service;

/**
 * Bucket 操作控制器
 * <p>
 * 处理 S3 Bucket 相关的 API 请求
 */
@Slf4j
@RestController
@RequiredArgsConstructor
public class BucketController {

    private final S3Service s3Service;
    private final ChimeraProperties chimeraProperties;

    private static final String APPLICATION_XML = "application/xml";

    /**
     * List all buckets
     * GET /
     */
    @GetMapping(value = "/", produces = APPLICATION_XML)
    public Mono<ResponseEntity<ListAllBucketsResult>> listBuckets() {
        return s3Service.listBuckets()
                .collectList()
                .map(buckets -> {
                    ListAllBucketsResult.Owner owner = new ListAllBucketsResult.Owner("chimera", "S3Chimera");
                    ListAllBucketsResult result = new ListAllBucketsResult(owner, buckets);
                    return ResponseEntity.ok(result);
                });
    }

    /**
     * 创建Bucket
     * PUT /{bucket}
     */
    @PutMapping("/{bucket}")
    public Mono<ResponseEntity<Void>> createBucket(@PathVariable String bucket) {
        return s3Service.createBucket(bucket)
                .map(b -> ResponseEntity.ok().<Void>build())
                .onErrorResume(e -> Mono.just(ResponseEntity.status(HttpStatus.CONFLICT).build()));
    }

    /**
     * 删除Bucket
     * DELETE /{bucket}
     */
    @DeleteMapping("/{bucket}")
    public Mono<ResponseEntity<Void>> deleteBucket(@PathVariable String bucket) {
        return s3Service.deleteBucket(bucket)
                .then(Mono.just(ResponseEntity.noContent().<Void>build()))
                .onErrorResume(e -> Mono.just(ResponseEntity.notFound().build()));
    }

    /**
     * 检查Bucket是否存在 (HEAD)
     * HEAD /{bucket}
     */
    @RequestMapping(value = "/{bucket}", method = RequestMethod.HEAD)
    public Mono<ResponseEntity<Void>> headBucket(@PathVariable String bucket) {
        return s3Service.bucketExists(bucket)
                .map(exists -> exists
                        ? ResponseEntity.ok().<Void>build()
                        : ResponseEntity.notFound().<Void>build());
    }

    /**
     * 获取Bucket ACL
     * GET /{bucket}?acl
     */
    @GetMapping(value = "/{bucket}", params = "acl", produces = APPLICATION_XML)
    public Mono<ResponseEntity<AccessControlPolicy>> getBucketAcl(@PathVariable String bucket) {
        return s3Service.bucketExists(bucket)
                .flatMap(exists -> {
                    if (!exists) {
                        return Mono.error(new win.ixuni.chimera.core.exception.BucketNotFoundException(bucket));
                    }
                    // Return default ACL (owner has full control)
                    AccessControlPolicy acl = AccessControlPolicy.createDefaultFullControl("chimera", "S3Chimera");
                    return Mono.just(ResponseEntity.ok(acl));
                });
    }

    /**
     * Get bucket region location
     * GET /{bucket}?location
     */
    @GetMapping(value = "/{bucket}", params = "location", produces = APPLICATION_XML)
    public Mono<ResponseEntity<LocationConstraint>> getBucketLocation(@PathVariable String bucket) {
        return s3Service.bucketExists(bucket)
                .flatMap(exists -> {
                    if (!exists) {
                        return Mono.error(new win.ixuni.chimera.core.exception.BucketNotFoundException(bucket));
                    }
                    String region = chimeraProperties.getServer().getRegion();
                    LocationConstraint location = new LocationConstraint(region);
                    return Mono.just(ResponseEntity.ok(location));
                });
    }

    /**
     * 列出Bucket中的对象 (V1 & V2)
     * GET /{bucket}
     */
    @GetMapping(value = "/{bucket}", produces = APPLICATION_XML)
    public Mono<ResponseEntity<?>> listObjects(
            @PathVariable String bucket,
            @RequestParam(required = false) String prefix,
            @RequestParam(required = false) String delimiter,
            @RequestParam(required = false) String marker,
            @RequestParam(name = "max-keys", required = false, defaultValue = "1000") Integer maxKeys,
            @RequestParam(name = "list-type", required = false) Integer listType,
            @RequestParam(name = "continuation-token", required = false) String continuationToken,
            @RequestParam(name = "start-after", required = false) String startAfter,
            @RequestParam(name = "fetch-owner", required = false) Boolean fetchOwner) {

        boolean isV2 = listType != null && listType == 2;

        ListObjectsRequest request = ListObjectsRequest.builder()
                .bucketName(bucket)
                .prefix(prefix)
                .delimiter(delimiter)
                .marker(marker)
                .maxKeys(maxKeys)
                .continuationToken(continuationToken)
                .startAfter(startAfter)
                .fetchOwner(fetchOwner)
                .useV2(isV2)
                .build();

        if (isV2) {
            return s3Service.listObjectsV2(request)
                    .map(ResponseEntity::ok);
        } else {
            return s3Service.listObjects(request)
                    .map(ResponseEntity::ok);
        }
    }

    /**
     * 批量删除对象
     * POST /{bucket}?delete
     */
    @PostMapping(value = "/{bucket}", params = "delete", produces = APPLICATION_XML, consumes = APPLICATION_XML)
    public Mono<ResponseEntity<DeleteObjectsResult>> deleteObjects(
            @PathVariable String bucket,
            @RequestBody DeleteObjectsXmlRequest request) {

        DeleteObjectsRequest deleteRequest = DeleteObjectsRequest.builder()
                .bucketName(bucket)
                .keys(request.getObjects().stream()
                        .map(DeleteObjectsXmlRequest.ObjectIdentifier::getKey)
                        .toList())
                .quiet(request.getQuiet() != null && request.getQuiet())
                .build();

        return s3Service.deleteObjects(deleteRequest)
                .map(ResponseEntity::ok);
    }

    /**
     * 批量删除请求 (XML格式)
     */
    @lombok.Data
    @com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement(localName = "Delete")
    public static class DeleteObjectsXmlRequest {
        @com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty(localName = "Quiet")
        private Boolean quiet;

        @com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper(useWrapping = false)
        @com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty(localName = "Object")
        private java.util.List<ObjectIdentifier> objects;

        @lombok.Data
        public static class ObjectIdentifier {
            @com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty(localName = "Key")
            private String key;

            @com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty(localName = "VersionId")
            private String versionId;
        }
    }
}
