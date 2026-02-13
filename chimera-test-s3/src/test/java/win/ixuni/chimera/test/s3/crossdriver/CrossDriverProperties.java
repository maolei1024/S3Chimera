package win.ixuni.chimera.test.s3.crossdriver;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;

@Data
@Configuration
@ConfigurationProperties(prefix = "chimera")
public class CrossDriverProperties {

    /**
     * 跨驱动测试配置
     */
    private CrossDriverConfig crossdriver = new CrossDriverConfig();

    /**
     * 驱动列表
     */
    private List<DriverConfig> drivers = new ArrayList<>();

    @Data
    public static class CrossDriverConfig {
        /**
         * Random sample count; set to -1 to run all combinations
         */
        private int pickCount = 5;

        /**
         * 大文件测试大小 (字节)，默认 100MB
         */
        private long largeFileSize = 104857600L;

        /**
         * 小文件测试大小 (字节)，默认 10KB
         */
        private int smallFileSize = 10240;
    }
}
