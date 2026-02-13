package win.ixuni.chimera.driver.s3.context;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import win.ixuni.chimera.core.config.DriverConfig;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandlerRegistry;

/**
 * S3 代理驱动上下文
 * <p>
 * Holds the AWS S3 async client and configuration
 */
@Getter
@Builder
public class S3ProxyDriverContext implements DriverContext {

    private final DriverConfig config;

    /**
     * AWS S3 async client
     */
    private final S3AsyncClient s3Client;

    /**
     * Operation handler registry (injected at runtime)
     */
    @Setter
    private OperationHandlerRegistry handlerRegistry;

    @Override
    public DriverConfig getConfig() {
        return config;
    }

    @Override
    public String getDriverName() {
        return config.getName();
    }

    @Override
    public String getDriverType() {
        return "s3";
    }
}
