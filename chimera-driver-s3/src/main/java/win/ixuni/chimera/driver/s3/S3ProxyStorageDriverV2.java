package win.ixuni.chimera.driver.s3;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import win.ixuni.chimera.core.config.DriverConfig;
import win.ixuni.chimera.core.driver.AbstractStorageDriverV2;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.driver.s3.context.S3ProxyDriverContext;
import win.ixuni.chimera.driver.s3.handler.bucket.*;
import win.ixuni.chimera.driver.s3.handler.multipart.*;
import win.ixuni.chimera.driver.s3.handler.object.*;
import win.ixuni.chimera.driver.s3.interceptor.S3ExceptionTranslationInterceptor;

import java.net.URI;

/**
 * S3 proxy storage driver V2
 * <p>
 * 代理标准 S3 后端（MinIO、AWS S3、阿里云 OSS 等）。
 */
@Slf4j
public class S3ProxyStorageDriverV2 extends AbstractStorageDriverV2 {

    @Getter
    private final DriverConfig config;

    private final S3ProxyDriverContext driverContext;
    private final S3AsyncClient s3Client;

    public S3ProxyStorageDriverV2(DriverConfig config) {
        this.config = config;

        // 构建 S3 客户端
        this.s3Client = buildS3Client(config);

        this.driverContext = S3ProxyDriverContext.builder()
                .config(config)
                .s3Client(s3Client)
                .build();

        registerHandlers();
        registerInterceptors();
        driverContext.setHandlerRegistry(getHandlerRegistry());
    }

    private S3AsyncClient buildS3Client(DriverConfig config) {
        String endpoint = config.getString("endpoint", null);
        String accessKey = config.getString("access-key", "");
        String secretKey = config.getString("secret-key", "");
        String region = config.getString("region", "us-east-1");
        boolean pathStyle = config.getBoolean("path-style", true);

        // 校验必要参数
        if (endpoint == null || endpoint.isBlank()) {
            log.warn("S3 driver '{}': no endpoint configured, will use AWS default", config.getName());
        }
        if (accessKey.isBlank() || secretKey.isBlank()) {
            throw new IllegalArgumentException(
                    "S3 driver '" + config.getName() + "': access-key and secret-key must be configured");
        }

        var builder = S3AsyncClient.builder()
                .region(Region.of(region))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(accessKey, secretKey)))
                .forcePathStyle(pathStyle);

        if (endpoint != null && !endpoint.isEmpty()) {
            builder.endpointOverride(URI.create(endpoint));
        }

        return builder.build();
    }

    private void registerHandlers() {
        // Bucket handlers (4)
        getHandlerRegistry().register(new S3CreateBucketHandler());
        getHandlerRegistry().register(new S3DeleteBucketHandler());
        getHandlerRegistry().register(new S3BucketExistsHandler());
        getHandlerRegistry().register(new S3ListBucketsHandler());

        // Object handlers (9)
        getHandlerRegistry().register(new S3PutObjectHandler());
        getHandlerRegistry().register(new S3GetObjectHandler());
        getHandlerRegistry().register(new S3HeadObjectHandler());
        getHandlerRegistry().register(new S3DeleteObjectHandler());
        getHandlerRegistry().register(new S3ListObjectsHandler());
        getHandlerRegistry().register(new S3ListObjectsV2Handler());
        getHandlerRegistry().register(new S3CopyObjectHandler());
        getHandlerRegistry().register(new S3DeleteObjectsHandler());
        getHandlerRegistry().register(new S3GetObjectRangeHandler());

        // Multipart handlers (6)
        getHandlerRegistry().register(new S3CreateMultipartUploadHandler());
        getHandlerRegistry().register(new S3UploadPartHandler());
        getHandlerRegistry().register(new S3CompleteMultipartUploadHandler());
        getHandlerRegistry().register(new S3AbortMultipartUploadHandler());
        getHandlerRegistry().register(new S3ListPartsHandler());
        getHandlerRegistry().register(new S3ListMultipartUploadsHandler());

        log.info("Registered {} operation handlers for S3 proxy driver", getHandlerRegistry().size());
    }

    private void registerInterceptors() {
        getHandlerRegistry().addInterceptor(new S3ExceptionTranslationInterceptor());
        log.debug("Registered S3 exception translation interceptor");
    }

    @Override
    public DriverContext getDriverContext() {
        return driverContext;
    }

    @Override
    public String getDriverType() {
        return S3ProxyDriverFactory.DRIVER_TYPE;
    }

    @Override
    public String getDriverName() {
        return config.getName();
    }

    @Override
    public Mono<Void> initialize() {
        log.info("Initializing S3 proxy driver: {} -> {}",
                config.getName(),
                config.getString("endpoint", "AWS S3"));
        return Mono.empty();
    }

    @Override
    public Mono<Void> shutdown() {
        log.info("Shutting down S3 proxy driver: {}", config.getName());
        s3Client.close();
        return Mono.empty();
    }
}
