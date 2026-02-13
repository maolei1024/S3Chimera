package win.ixuni.chimera.driver.webdav;

import com.github.sardine.SardineFactory;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.config.DriverConfig;
import win.ixuni.chimera.core.driver.AbstractStorageDriverV2;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.driver.webdav.context.WebDavDriverContext;
import win.ixuni.chimera.driver.webdav.handler.bucket.*;
import win.ixuni.chimera.driver.webdav.handler.multipart.*;
import win.ixuni.chimera.driver.webdav.handler.object.*;

/**
 * WebDAV storage driver V2
 * <p>
 * 将 WebDAV 服务器暴露为 S3 兼容接口。
 * Uses only basic WebDAV methods (GET/PUT/DELETE/MKCOL/PROPFIND),
 * Does not rely on COPY/MOVE extension methods to ensure compatibility.
 */
@Slf4j
public class WebDavStorageDriverV2 extends AbstractStorageDriverV2 {

    @Getter
    private final DriverConfig config;

    private final WebDavDriverContext driverContext;

    public WebDavStorageDriverV2(DriverConfig config) {
        this.config = config;

        String url = config.getString("url", "");
        String username = config.getString("username", "");
        String password = config.getString("password", "");

        var sardine = SardineFactory.begin(username, password);

        this.driverContext = WebDavDriverContext.builder()
                .config(config)
                .sardine(sardine)
                .baseUrl(url)
                .build();

        registerHandlers();
        driverContext.setHandlerRegistry(getHandlerRegistry());
    }

    private void registerHandlers() {
        // Bucket handlers (4)
        getHandlerRegistry().register(new WebDavCreateBucketHandler());
        getHandlerRegistry().register(new WebDavDeleteBucketHandler());
        getHandlerRegistry().register(new WebDavBucketExistsHandler());
        getHandlerRegistry().register(new WebDavListBucketsHandler());

        // Object handlers (9)
        getHandlerRegistry().register(new WebDavPutObjectHandler());
        getHandlerRegistry().register(new WebDavGetObjectHandler());
        getHandlerRegistry().register(new WebDavGetObjectRangeHandler());
        getHandlerRegistry().register(new WebDavHeadObjectHandler());
        getHandlerRegistry().register(new WebDavDeleteObjectHandler());
        getHandlerRegistry().register(new WebDavDeleteObjectsHandler());
        getHandlerRegistry().register(new WebDavListObjectsHandler());
        getHandlerRegistry().register(new WebDavListObjectsV2Handler());
        getHandlerRegistry().register(new WebDavCopyObjectHandler());

        // Multipart handlers (6)
        getHandlerRegistry().register(new WebDavCreateMultipartUploadHandler());
        getHandlerRegistry().register(new WebDavUploadPartHandler());
        getHandlerRegistry().register(new WebDavCompleteMultipartUploadHandler());
        getHandlerRegistry().register(new WebDavAbortMultipartUploadHandler());
        getHandlerRegistry().register(new WebDavListPartsHandler());
        getHandlerRegistry().register(new WebDavListMultipartUploadsHandler());

        log.info("Registered {} operation handlers for WebDAV driver", getHandlerRegistry().size());
    }

    @Override
    public DriverContext getDriverContext() {
        return driverContext;
    }

    @Override
    public String getDriverType() {
        return WebDavDriverFactory.DRIVER_TYPE;
    }

    @Override
    public String getDriverName() {
        return config.getName();
    }

    @Override
    public Mono<Void> initialize() {
        log.info("Initializing WebDAV driver: {} -> {}",
                config.getName(),
                config.getString("url", ""));
        return Mono.empty();
    }

    @Override
    public Mono<Void> shutdown() {
        log.info("Shutting down WebDAV driver: {}", config.getName());
        try {
            driverContext.getSardine().shutdown();
        } catch (Exception e) {
            log.warn("Error shutting down Sardine client", e);
        }
        return Mono.empty();
    }
}
