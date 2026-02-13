package win.ixuni.chimera.core.util;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * S3 protocol validation utility class
 */
public class S3ValidationUtils {

    /**
     * Verify user metadata
     * <p>
     * AWS S3 limits:
     * - Total size of all user metadata must not exceed 2KB (2048 bytes)
     * - Metadata key 只能包含 ASCII 字符
     *
     * @param metadata user metadata
     * @return 错误信息，如果验证通过返回 null
     */
    public static String validateMetadata(Map<String, String> metadata) {
        if (metadata == null || metadata.isEmpty()) {
            return null;
        }

        int totalSize = 0;
        for (Map.Entry<String, String> entry : metadata.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();

            // Check whether the key contains only ASCII characters
            if (!isAscii(key)) {
                return "Metadata key contains non-ASCII characters: " + key;
            }

            // Accumulate size (key + value byte count)
            totalSize += key.getBytes(StandardCharsets.UTF_8).length;
            if (value != null) {
                totalSize += value.getBytes(StandardCharsets.UTF_8).length;
            }
        }

        // AWS S3 limit: 2KB = 2048 bytes
        if (totalSize > 2048) {
            return "User metadata exceeds 2KB limit (actual: " + totalSize + " bytes)";
        }

        return null;
    }

    /**
     * Check whether a string contains only ASCII characters
     */
    public static boolean isAscii(String str) {
        if (str == null) {
            return true;
        }
        for (char c : str.toCharArray()) {
            if (c > 127) {
                return false;
            }
        }
        return true;
    }
}
