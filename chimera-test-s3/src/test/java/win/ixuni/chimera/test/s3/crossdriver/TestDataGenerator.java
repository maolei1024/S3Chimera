package win.ixuni.chimera.test.s3.crossdriver;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;

/**
 * 测试数据生成器
 */
public class TestDataGenerator {

    private static final Random RANDOM = new Random();

    /**
     * 生成随机字节数组
     */
    public static byte[] generateRandomBytes(int size) {
        byte[] bytes = new byte[size];
        RANDOM.nextBytes(bytes);
        return bytes;
    }

    /**
     * 计算 MD5 校验和
     */
    public static String calculateMD5(byte[] data) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] digest = md.digest(data);
            StringBuilder sb = new StringBuilder();
            for (byte b : digest) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 algorithm not found", e);
        }
    }

    /**
     * 计算 MD5 校验和 (十六进制字符串)
     */
    public static String calculateMD5Hex(byte[] data) {
        return calculateMD5(data);
    }
}
