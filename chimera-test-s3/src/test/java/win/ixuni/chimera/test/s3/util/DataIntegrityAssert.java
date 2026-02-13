package win.ixuni.chimera.test.s3.util;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 数据完整性断言工具类
 * <p>
 * Provides detailed data comparison assertion methods with useful diagnostic info on failure.
 */
public final class DataIntegrityAssert {

    private DataIntegrityAssert() {
        // 工具类不允许实例化
    }

    // ==================== 内容断言 ====================

    /**
     * 断言二进制内容完全相等
     * <p>
     * Provides detailed diff info on failure (size mismatch, first differing byte position, etc.)
     *
     * @param expected 期望的数据
     * @param actual   实际的数据
     */
    public static void assertContentEquals(byte[] expected, byte[] actual) {
        assertContentEquals(expected, actual, "Content mismatch");
    }

    /**
     * Assert binary content is exactly equal (with custom message)
     *
     * @param expected 期望的数据
     * @param actual   实际的数据
     * @param message  断言失败时的前缀消息
     */
    public static void assertContentEquals(byte[] expected, byte[] actual, String message) {
        assertNotNull(expected, message + ": expected content is null");
        assertNotNull(actual, message + ": actual content is null");

        if (expected.length != actual.length) {
            fail(String.format("%s: Size mismatch - expected %d bytes, but got %d bytes",
                    message, expected.length, actual.length));
        }

        // Find the first differing position
        for (int i = 0; i < expected.length; i++) {
            if (expected[i] != actual[i]) {
                // Provide context: show bytes surrounding the diff location
                int contextStart = Math.max(0, i - 5);
                int contextEnd = Math.min(expected.length, i + 10);

                String expectedContext = bytesToHexWithHighlight(expected, contextStart, contextEnd, i);
                String actualContext = bytesToHexWithHighlight(actual, contextStart, contextEnd, i);

                fail(String.format(
                        "%s: Content differs at byte %d (0x%X)%n" +
                                "  Expected byte: 0x%02X%n" +
                                "  Actual byte:   0x%02X%n" +
                                "  Context (bytes %d-%d, diff at [*]):%n" +
                                "    Expected: %s%n" +
                                "    Actual:   %s",
                        message, i, i, expected[i] & 0xFF, actual[i] & 0xFF,
                        contextStart, contextEnd, expectedContext, actualContext));
            }
        }

        // Additional validation: use MD5 to ensure integrity
        String expectedMd5 = TestDataGenerator.calculateMD5(expected);
        String actualMd5 = TestDataGenerator.calculateMD5(actual);
        assertEquals(expectedMd5, actualMd5,
                message + ": MD5 checksum mismatch (content appears equal but checksums differ)");
    }

    private static String bytesToHexWithHighlight(byte[] data, int start, int end, int highlightIndex) {
        StringBuilder sb = new StringBuilder();
        for (int i = start; i < end && i < data.length; i++) {
            if (i == highlightIndex) {
                sb.append("[*");
            }
            sb.append(String.format("%02X", data[i] & 0xFF));
            if (i == highlightIndex) {
                sb.append("*]");
            }
            sb.append(" ");
        }
        return sb.toString().trim();
    }

    // ==================== 字符串内容断言 ====================

    /**
     * Assert string content is equal, showing diff location on failure
     *
     * @param expected 期望的字符串
     * @param actual   实际的字符串
     */
    public static void assertStringContentEquals(String expected, String actual) {
        assertStringContentEquals(expected, actual, "String content mismatch");
    }

    /**
     * Assert string content is equal (with custom message)
     *
     * @param expected 期望的字符串
     * @param actual   实际的字符串
     * @param message  断言失败时的前缀消息
     */
    public static void assertStringContentEquals(String expected, String actual, String message) {
        assertNotNull(expected, message + ": expected string is null");
        assertNotNull(actual, message + ": actual string is null");

        if (expected.length() != actual.length()) {
            fail(String.format("%s: Length mismatch - expected %d chars, but got %d chars",
                    message, expected.length(), actual.length()));
        }

        for (int i = 0; i < expected.length(); i++) {
            if (expected.charAt(i) != actual.charAt(i)) {
                // 显示上下文
                int contextStart = Math.max(0, i - 10);
                int contextEnd = Math.min(expected.length(), i + 20);

                fail(String.format(
                        "%s: String differs at position %d%n" +
                                "  Expected char: '%c' (U+%04X)%n" +
                                "  Actual char:   '%c' (U+%04X)%n" +
                                "  Context:%n" +
                                "    Expected: ...%s...%n" +
                                "    Actual:   ...%s...",
                        message, i,
                        expected.charAt(i), (int) expected.charAt(i),
                        actual.charAt(i), (int) actual.charAt(i),
                        expected.substring(contextStart, contextEnd),
                        actual.substring(contextStart, contextEnd)));
            }
        }

        assertEquals(expected, actual, message);
    }

    // ==================== 元数据断言 ====================

    /**
     * Assert S3 user metadata is exactly equal
     *
     * @param expected 期望的元数据
     * @param actual   实际的元数据
     */
    public static void assertMetadataEquals(Map<String, String> expected, Map<String, String> actual) {
        assertMetadataEquals(expected, actual, "Metadata mismatch");
    }

    /**
     * Assert S3 user metadata is exactly equal (with custom message)
     *
     * @param expected 期望的元数据
     * @param actual   实际的元数据
     * @param message  断言失败时的前缀消息
     */
    public static void assertMetadataEquals(Map<String, String> expected, Map<String, String> actual, String message) {
        assertNotNull(expected, message + ": expected metadata is null");
        assertNotNull(actual, message + ": actual metadata is null");

        // Check if all expected keys exist
        for (Map.Entry<String, String> entry : expected.entrySet()) {
            String key = entry.getKey();
            String expectedValue = entry.getValue();

            // S3 元数据 key 可能被转换为小写
            String actualValue = actual.get(key);
            if (actualValue == null) {
                actualValue = actual.get(key.toLowerCase());
            }

            assertNotNull(actualValue,
                    String.format("%s: Missing metadata key '%s'. Available keys: %s",
                            message, key, actual.keySet()));

            assertEquals(expectedValue, actualValue,
                    String.format("%s: Metadata value mismatch for key '%s'", message, key));
        }

        // Optional: check for extra keys
        // Note: S3 may add system metadata, so no strict check here
    }

    // ==================== ETag 断言 ====================

    /**
     * Assert ETag is valid (non-empty and correctly formatted)
     *
     * @param etag ETag 值
     */
    public static void assertETagValid(String etag) {
        assertNotNull(etag, "ETag should not be null");
        assertFalse(etag.isBlank(), "ETag should not be blank");

        // Remove quotes (if present)
        String cleanEtag = etag.replace("\"", "");
        assertFalse(cleanEtag.isEmpty(), "ETag should not be empty after removing quotes");
    }

    /**
     * Assert ETag matches content MD5
     * <p>
     * Note: multipart upload ETag format is "md5-partCount"; this method only applies to single uploads
     *
     * @param etag    ETag 值
     * @param content 对象内容
     */
    public static void assertETagMatchesContent(String etag, byte[] content) {
        assertETagValid(etag);

        String cleanEtag = etag.replace("\"", "").toLowerCase();

        // 如果是分片上传 ETag（包含 "-"），跳过 MD5 验证
        if (cleanEtag.contains("-")) {
            // 分片上传的 ETag，只检查格式
            assertTrue(cleanEtag.matches("[a-f0-9]+-\\d+"),
                    "Multipart ETag should match pattern 'md5hex-partCount', but got: " + etag);
            return;
        }

        // Single upload ETag should be the content MD5
        String expectedMd5 = TestDataGenerator.calculateMD5(content).toLowerCase();
        assertEquals(expectedMd5, cleanEtag,
                "ETag should match content MD5 for single-part upload");
    }

    // ==================== 大小断言 ====================

    /**
     * 断言内容大小
     *
     * @param expected 期望的大小
     * @param actual   实际的数据
     */
    public static void assertContentSize(long expected, byte[] actual) {
        assertNotNull(actual, "Content should not be null");
        assertEquals(expected, actual.length,
                String.format("Content size mismatch: expected %d bytes, got %d bytes", expected, actual.length));
    }

    /**
     * Assert Content-Length header matches actual content
     *
     * @param contentLength Content-Length 头部值
     * @param content       实际内容
     */
    public static void assertContentLengthMatches(Long contentLength, byte[] content) {
        assertNotNull(contentLength, "Content-Length should not be null");
        assertNotNull(content, "Content should not be null");
        assertEquals(contentLength.intValue(), content.length,
                String.format("Content-Length header (%d) doesn't match actual content size (%d)",
                        contentLength, content.length));
    }
}
