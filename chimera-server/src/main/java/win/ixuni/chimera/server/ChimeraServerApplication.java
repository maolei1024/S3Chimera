package win.ixuni.chimera.server;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;
import win.ixuni.chimera.core.config.ChimeraProperties;

/**
 * S3Chimera 服务器启动类
 * 
 * Exclude R2DBC auto-configuration since database drivers are optional and managed by each driver module
 */
@SpringBootApplication(
        scanBasePackages = "win.ixuni.chimera",
        excludeName = {
                "org.springframework.boot.autoconfigure.r2dbc.R2dbcAutoConfiguration",
                "org.springframework.boot.autoconfigure.data.r2dbc.R2dbcDataAutoConfiguration",
                "org.springframework.boot.autoconfigure.r2dbc.R2dbcTransactionManagerAutoConfiguration"
        }
)
@EnableConfigurationProperties(ChimeraProperties.class)
@EnableScheduling
public class ChimeraServerApplication {

    public static void main(String[] args) {
        SpringApplication.run(ChimeraServerApplication.class, args);
    }
}
