package win.ixuni.chimera.test.s3.util;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * 测试数据生成器
 * <p>
 * Generates various types of test data, supporting random binary and text data generation,
 * and MD5/SHA256 checksum computation.
 */
public final class TestDataGenerator {

    private static final SecureRandom RANDOM = new SecureRandom();

    // ASCII printable character set (for text generation)
    private static final String ASCII_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789 !@#$%^&*()_+-=[]{}|;':\",./<>?\n\t";

    // Unicode 测试字符（包含中文、日文、韩文、Emoji 等）
    private static final String UNICODE_CHARS = "你好世界こんにちは안녕하세요🎉🚀💻🔥✨αβγδεζηθ";

    private TestDataGenerator() {
        // 工具类不允许实例化
    }

    // ==================== 随机数据生成 ====================

    /**
     * 生成指定大小的随机二进制数据
     *
     * @param sizeBytes 数据大小（字节）
     * @return 随机字节数组
     */
    public static byte[] generateRandomBytes(int sizeBytes) {
        byte[] data = new byte[sizeBytes];
        RANDOM.nextBytes(data);
        return data;
    }

    /**
     * 生成指定大小的随机 ASCII 文本
     *
     * @param sizeBytes 数据大小（字节）
     * @return 随机 ASCII 文本
     */
    public static String generateRandomAsciiText(int sizeBytes) {
        StringBuilder sb = new StringBuilder(sizeBytes);
        for (int i = 0; i < sizeBytes; i++) {
            sb.append(ASCII_CHARS.charAt(RANDOM.nextInt(ASCII_CHARS.length())));
        }
        return sb.toString();
    }

    /**
     * 生成包含 Unicode 字符的随机文本
     *
     * @param charCount 字符数量
     * @return 随机 Unicode 文本
     */
    public static String generateRandomUnicodeText(int charCount) {
        StringBuilder sb = new StringBuilder(charCount);
        String combined = ASCII_CHARS + UNICODE_CHARS;
        for (int i = 0; i < charCount; i++) {
            sb.append(combined.charAt(RANDOM.nextInt(combined.length())));
        }
        return sb.toString();
    }

    /**
     * Generate user metadata for testing
     *
     * @return 包含多种类型值的元数据 Map
     */
    public static Map<String, String> generateTestMetadata() {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("test-id", UUID.randomUUID().toString());
        metadata.put("created-by", "S3DataIntegrityTest");
        metadata.put("timestamp", String.valueOf(System.currentTimeMillis()));
        metadata.put("custom-value", "value-with-特殊字符-123");
        return metadata;
    }

    /**
     * Generate filenames with special characters
     *
     * @param type 文件名类型
     * @return 特殊文件名
     */
    public static String generateSpecialFileName(FileNameType type) {
        String uuid = UUID.randomUUID().toString().substring(0, 8);
        return switch (type) {
            case CHINESE -> "测试文件-" + uuid + ".txt";
            case JAPANESE -> "テスト-" + uuid + ".txt";
            case KOREAN -> "테스트-" + uuid + ".txt";
            case EMOJI -> "🎉test-" + uuid + "-🚀.txt";
            case SPECIAL_CHARS -> "test!@#$%^&()-" + uuid + ".txt";
            case SPACES -> "test file with spaces " + uuid + ".txt";
            case DEEP_PATH -> "level1/level2/level3/level4/level5/file-" + uuid + ".txt";
            case LONG_NAME -> "a".repeat(200) + "-" + uuid + ".txt";
        };
    }

    public enum FileNameType {
        CHINESE, JAPANESE, KOREAN, EMOJI, SPECIAL_CHARS, SPACES, DEEP_PATH, LONG_NAME
    }

    // ==================== 校验和计算 ====================

    /**
     * 计算 MD5 校验和（返回 hex 字符串）
     *
     * @param data 数据
     * @return MD5 hex 字符串
     */
    public static String calculateMD5(byte[] data) {
        return calculateHash(data, "MD5");
    }

    /**
     * 计算 MD5 校验和（返回 Base64 字符串）
     *
     * @param data 数据
     * @return MD5 Base64 字符串
     */
    public static String calculateMD5Base64(byte[] data) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] hash = md.digest(data);
            return Base64.getEncoder().encodeToString(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 algorithm not found", e);
        }
    }

    /**
     * 计算 SHA256 校验和
     *
     * @param data 数据
     * @return SHA256 hex 字符串
     */
    public static String calculateSHA256(byte[] data) {
        return calculateHash(data, "SHA-256");
    }

    private static String calculateHash(byte[] data, String algorithm) {
        try {
            MessageDigest md = MessageDigest.getInstance(algorithm);
            byte[] hash = md.digest(data);
            return bytesToHex(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(algorithm + " algorithm not found", e);
        }
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    // ==================== 封装数据对象 ====================

    /**
     * Generate test file data with complete metadata
     *
     * @param prefix    文件名前缀
     * @param sizeBytes 文件大小
     * @return TestFile 对象
     */
    public static TestFile generateTestFile(String prefix, int sizeBytes) {
        byte[] content = generateRandomBytes(sizeBytes);
        String key = prefix + "-" + UUID.randomUUID().toString().substring(0, 8) + ".bin";
        return new TestFile(key, content, calculateMD5(content), calculateSHA256(content));
    }

    /**
     * 生成文本测试文件
     *
     * @param prefix    文件名前缀
     * @param sizeBytes 文件大小
     * @return TestFile 对象
     */
    public static TestFile generateTextTestFile(String prefix, int sizeBytes) {
        String textContent = generateRandomAsciiText(sizeBytes);
        byte[] content = textContent.getBytes(StandardCharsets.UTF_8);
        String key = prefix + "-" + UUID.randomUUID().toString().substring(0, 8) + ".txt";
        return new TestFile(key, content, calculateMD5(content), calculateSHA256(content));
    }

    /**
     * 批量生成测试文件
     *
     * @param count     文件数量
     * @param sizeBytes 每个文件的大小
     * @return TestFile 数组
     */
    public static TestFile[] generateBulkTestFiles(int count, int sizeBytes) {
        TestFile[] files = new TestFile[count];
        for (int i = 0; i < count; i++) {
            files[i] = generateTestFile("bulk-" + i, sizeBytes);
        }
        return files;
    }

    /**
     * 生成随机大小的测试文件
     *
     * @param prefix   文件名前缀
     * @param minBytes 最小大小（字节）
     * @param maxBytes 最大大小（字节）
     * @return TestFile 对象
     */
    public static TestFile generateRandomSizeTestFile(String prefix, int minBytes, int maxBytes) {
        int size = minBytes + RANDOM.nextInt(maxBytes - minBytes + 1);
        return generateTestFile(prefix, size);
    }

    /**
     * 生成混合大小的批量测试文件
     *
     * @param count    文件数量
     * @param minBytes 最小大小
     * @param maxBytes 最大大小
     * @return TestFile 数组
     */
    public static TestFile[] generateMixedSizeTestFiles(int count, int minBytes, int maxBytes) {
        TestFile[] files = new TestFile[count];
        for (int i = 0; i < count; i++) {
            files[i] = generateRandomSizeTestFile("mixed-" + i, minBytes, maxBytes);
        }
        return files;
    }

    /**
     * 测试文件封装类
     */
    public record TestFile(
            String key,
            byte[] content,
            String md5Hex,
            String sha256Hex) {
        public int size() {
            return content.length;
        }

        public String contentAsString() {
            return new String(content, StandardCharsets.UTF_8);
        }
    }
}
