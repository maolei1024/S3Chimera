package win.ixuni.chimera.server.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.StorageDriverV2;
import win.ixuni.chimera.core.model.*;
import win.ixuni.chimera.core.operation.bucket.*;
import win.ixuni.chimera.core.operation.multipart.*;
import win.ixuni.chimera.core.operation.object.*;
import win.ixuni.chimera.server.registry.DriverRegistry;
import win.ixuni.chimera.server.routing.BucketRouter;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * S3 service layer
 * <p>
 * Provides S3 operations business logic, coordinating router and drivers.
 * All operations are executed via the execute(Operation) pattern.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class S3Service {

        private final BucketRouter bucketRouter;
        private final DriverRegistry driverRegistry;

        // ==================== Bucket 操作 ====================

        public Mono<S3Bucket> createBucket(String bucketName) {
                log.info("Creating bucket: {}", bucketName);
                return bucketRouter.route(bucketName)
                                .execute(new CreateBucketOperation(bucketName));
        }

        public Mono<Void> deleteBucket(String bucketName) {
                log.info("Deleting bucket: {}", bucketName);
                return bucketRouter.route(bucketName)
                                .execute(new DeleteBucketOperation(bucketName));
        }

        public Mono<Boolean> bucketExists(String bucketName) {
                return bucketRouter.route(bucketName)
                                .execute(new BucketExistsOperation(bucketName));
        }

        public Flux<S3Bucket> listBuckets() {
                log.debug("Listing buckets from routable drivers");

                // Collect routable driver names: default driver + drivers from routing rules
                java.util.Set<String> routableDrivers = new java.util.HashSet<>();

                // Add default driver
                String defaultDriver = bucketRouter.getDefaultDriverName();
                if (defaultDriver != null && !defaultDriver.isEmpty()) {
                        routableDrivers.add(defaultDriver);
                }

                // Add drivers from routing rules
                bucketRouter.getRoutingRuleDriverNames().forEach(routableDrivers::add);

                log.debug("Routable drivers: {}", routableDrivers);

                return Flux.fromIterable(routableDrivers)
                                .flatMap(driverName -> {
                                        StorageDriverV2 driver = (StorageDriverV2) driverRegistry.getDriver(driverName);
                                        return driver.execute(new ListBucketsOperation())
                                                        .flatMapMany(flux -> flux)
                                                        .doOnNext(bucket -> bucket.setDriverName(driverName));
                                });
        }

        // ==================== Object 操作 ====================

        public Mono<S3Object> putObject(String bucketName, String key, Flux<ByteBuffer> content,
                        String contentType, Map<String, String> metadata) {
                log.info("Putting object: {}/{}", bucketName, key);
                return bucketRouter.route(bucketName)
                                .execute(PutObjectOperation.builder()
                                                .bucketName(bucketName)
                                                .key(key)
                                                .content(content)
                                                .contentType(contentType)
                                                .metadata(metadata)
                                                .build());
        }

        public Mono<S3ObjectData> getObject(String bucketName, String key) {
                log.debug("Getting object: {}/{}", bucketName, key);
                return bucketRouter.route(bucketName)
                                .execute(new GetObjectOperation(bucketName, key));
        }

        public Mono<S3ObjectData> getObjectRange(String bucketName, String key, long start, Long end) {
                log.debug("Getting object range: {}/{}, bytes={}-{}", bucketName, key, start, end);
                return bucketRouter.route(bucketName)
                                .execute(new GetObjectRangeOperation(bucketName, key, start, end));
        }

        public Mono<StorageDriverV2> getDriverForBucket(String bucketName) {
                return Mono.just(bucketRouter.route(bucketName));
        }

        public Mono<S3Object> headObject(String bucketName, String key) {
                log.debug("Head object: {}/{}", bucketName, key);
                return bucketRouter.route(bucketName)
                                .execute(new HeadObjectOperation(bucketName, key));
        }

        public Mono<Void> deleteObject(String bucketName, String key) {
                log.info("Deleting object: {}/{}", bucketName, key);
                return bucketRouter.route(bucketName)
                                .execute(new DeleteObjectOperation(bucketName, key));
        }

        public Mono<ListObjectsResult> listObjects(ListObjectsRequest request) {
                log.debug("Listing objects in bucket: {}", request.getBucketName());
                return bucketRouter.route(request.getBucketName())
                                .execute(new ListObjectsOperation(request));
        }

        public Mono<ListObjectsV2Result> listObjectsV2(ListObjectsRequest request) {
                log.debug("Listing objects V2 in bucket: {}", request.getBucketName());
                return bucketRouter.route(request.getBucketName())
                                .execute(new ListObjectsV2Operation(request));
        }

        public Mono<S3Object> copyObject(String sourceBucket, String sourceKey,
                        String destBucket, String destKey) {
                log.info("Copying object: {}/{} -> {}/{}", sourceBucket, sourceKey, destBucket, destKey);

                StorageDriverV2 srcDriver = bucketRouter.route(sourceBucket);
                StorageDriverV2 destDriver = bucketRouter.route(destBucket);

                // If same driver, use driver-internal optimized copy
                if (srcDriver.getDriverName().equals(destDriver.getDriverName())) {
                        return srcDriver.execute(new CopyObjectOperation(sourceBucket, sourceKey, destBucket, destKey));
                }

                // 跨驱动复制：读取源对象 -> 写入目标驱动
                log.info("Cross-driver copy: {} -> {}", srcDriver.getDriverName(), destDriver.getDriverName());
                return srcDriver.execute(new GetObjectOperation(sourceBucket, sourceKey))
                                .flatMap(objectData -> {
                                        S3Object metadata = objectData.getMetadata();
                                        return destDriver.execute(PutObjectOperation.builder()
                                                        .bucketName(destBucket)
                                                        .key(destKey)
                                                        .content(objectData.getContent())
                                                        .contentType(metadata.getContentType())
                                                        .metadata(metadata.getUserMetadata() != null
                                                                        ? metadata.getUserMetadata()
                                                                        : Map.of())
                                                        .build());
                                });
        }

        public Mono<DeleteObjectsResult> deleteObjects(DeleteObjectsRequest request) {
                log.info("Deleting {} objects from bucket: {}", request.getKeys().size(), request.getBucketName());
                return bucketRouter.route(request.getBucketName())
                                .execute(new DeleteObjectsOperation(request));
        }

        // ==================== 分片上传操作 ====================

        public Mono<MultipartUpload> createMultipartUpload(String bucketName, String key,
                        String contentType, Map<String, String> metadata) {
                log.info("Creating multipart upload: {}/{}", bucketName, key);
                return bucketRouter.route(bucketName)
                                .execute(CreateMultipartUploadOperation.builder()
                                                .bucketName(bucketName)
                                                .key(key)
                                                .contentType(contentType)
                                                .metadata(metadata)
                                                .build());
        }

        public Mono<UploadPart> uploadPart(String bucketName, String key, String uploadId,
                        Integer partNumber, Flux<ByteBuffer> content) {
                log.debug("Uploading part {} for {}/{}", partNumber, bucketName, key);
                return bucketRouter.route(bucketName)
                                .execute(new UploadPartOperation(bucketName, key, uploadId, partNumber, content));
        }

        public Mono<S3Object> completeMultipartUpload(String bucketName, String key, String uploadId,
                        List<CompletedPart> parts) {
                log.info("Completing multipart upload: {}/{}", bucketName, key);
                return bucketRouter.route(bucketName)
                                .execute(new CompleteMultipartUploadOperation(bucketName, key, uploadId, parts));
        }

        public Mono<Void> abortMultipartUpload(String bucketName, String key, String uploadId) {
                log.info("Aborting multipart upload: {}/{}", bucketName, key);
                return bucketRouter.route(bucketName)
                                .execute(new AbortMultipartUploadOperation(bucketName, key, uploadId));
        }

        public Mono<ListPartsResult> listParts(ListPartsRequest request) {
                log.debug("Listing parts for upload: {}", request.getUploadId());
                return bucketRouter.route(request.getBucketName())
                                .execute(new ListPartsOperation(request));
        }

        public Mono<ListMultipartUploadsResult> listMultipartUploads(String bucketName, String prefix) {
                log.debug("Listing multipart uploads for bucket: {}", bucketName);
                return bucketRouter.route(bucketName)
                                .execute(new ListMultipartUploadsOperation(bucketName, prefix));
        }
}
