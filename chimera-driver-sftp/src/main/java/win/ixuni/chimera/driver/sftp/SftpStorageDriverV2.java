package win.ixuni.chimera.driver.sftp;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.sshd.client.SshClient;
import org.apache.sshd.client.session.ClientSession;
import org.apache.sshd.sftp.client.SftpClient;
import org.apache.sshd.sftp.client.SftpClientFactory;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.config.DriverConfig;
import win.ixuni.chimera.core.driver.AbstractStorageDriverV2;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.driver.sftp.context.SftpDriverContext;
import win.ixuni.chimera.driver.sftp.handler.bucket.*;
import win.ixuni.chimera.driver.sftp.handler.multipart.*;
import win.ixuni.chimera.driver.sftp.handler.object.*;

import java.nio.file.Paths;
import java.security.KeyPair;
import java.time.Duration;

/**
 * SFTP storage driver V2
 * <p>
 * Exposes an SFTP server as an S3-compatible interface.
 * Buckets are mapped to directories, objects are mapped to files.
 */
@Slf4j
public class SftpStorageDriverV2 extends AbstractStorageDriverV2 {

    @Getter
    private final DriverConfig config;

    private SftpDriverContext driverContext;
    private SshClient sshClient;
    private ClientSession session;

    public SftpStorageDriverV2(DriverConfig config) {
        this.config = config;
    }

    private void registerHandlers() {
        // Bucket handlers (4)
        getHandlerRegistry().register(new SftpCreateBucketHandler());
        getHandlerRegistry().register(new SftpDeleteBucketHandler());
        getHandlerRegistry().register(new SftpBucketExistsHandler());
        getHandlerRegistry().register(new SftpListBucketsHandler());

        // Object handlers (9)
        getHandlerRegistry().register(new SftpPutObjectHandler());
        getHandlerRegistry().register(new SftpGetObjectHandler());
        getHandlerRegistry().register(new SftpGetObjectRangeHandler());
        getHandlerRegistry().register(new SftpHeadObjectHandler());
        getHandlerRegistry().register(new SftpDeleteObjectHandler());
        getHandlerRegistry().register(new SftpDeleteObjectsHandler());
        getHandlerRegistry().register(new SftpListObjectsHandler());
        getHandlerRegistry().register(new SftpListObjectsV2Handler());
        getHandlerRegistry().register(new SftpCopyObjectHandler());

        // Multipart handlers (6)
        getHandlerRegistry().register(new SftpCreateMultipartUploadHandler());
        getHandlerRegistry().register(new SftpUploadPartHandler());
        getHandlerRegistry().register(new SftpCompleteMultipartUploadHandler());
        getHandlerRegistry().register(new SftpAbortMultipartUploadHandler());
        getHandlerRegistry().register(new SftpListPartsHandler());
        getHandlerRegistry().register(new SftpListMultipartUploadsHandler());

        log.info("Registered {} operation handlers for SFTP driver", getHandlerRegistry().size());
    }

    @Override
    public DriverContext getDriverContext() {
        return driverContext;
    }

    @Override
    public String getDriverType() {
        return SftpDriverFactory.DRIVER_TYPE;
    }

    @Override
    public String getDriverName() {
        return config.getName();
    }

    @Override
    public Mono<Void> initialize() {
        String host = config.getString("host", "localhost");
        int port = config.getInt("port", 22);
        String username = config.getString("username", "");
        String password = config.getString("password", "");
        String privateKeyPath = config.getString("private-key", "");
        String basePath = config.getString("base-path", "/");

        log.info("Initializing SFTP driver: {} -> {}@{}:{}{}",
                config.getName(), username, host, port, basePath);

        return Mono.fromCallable(() -> {
            sshClient = SshClient.setUpDefaultClient();
            sshClient.start();

            session = sshClient.connect(username, host, port)
                    .verify(Duration.ofSeconds(30))
                    .getSession();

            // Authentication: password or private key
            if (!password.isEmpty()) {
                session.addPasswordIdentity(password);
            }
            if (!privateKeyPath.isEmpty()) {
                try {
                    var keyPairProvider = org.apache.sshd.common.keyprovider.FileKeyPairProvider.class
                            .getConstructor(java.nio.file.Path[].class)
                            .newInstance((Object) new java.nio.file.Path[] { Paths.get(privateKeyPath) });
                    for (KeyPair kp : keyPairProvider.loadKeys(null)) {
                        session.addPublicKeyIdentity(kp);
                    }
                } catch (Exception e) {
                    log.warn("Failed to load private key from {}: {}", privateKeyPath, e.getMessage());
                }
            }

            session.auth().verify(Duration.ofSeconds(30));

            // sftpClient is created on demand by context
            driverContext = SftpDriverContext.builder()
                    .config(config)
                    .session(session)
                    .basePath(basePath)
                    .build();

            registerHandlers();
            driverContext.setHandlerRegistry(getHandlerRegistry());

            log.info("SFTP driver initialized successfully: {}", config.getName());
            return null;
        }).then();
    }

    @Override
    public Mono<Void> shutdown() {
        log.info("Shutting down SFTP driver: {}", config.getName());
        return Mono.fromRunnable(() -> {
            try {
                if (session != null) {
                    session.close();
                }
                if (sshClient != null) {
                    sshClient.stop();
                }
            } catch (Exception e) {
                log.warn("Error shutting down SFTP client: {}", e.getMessage());
            }
        });
    }
}
