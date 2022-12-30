package top.wuzonghui.simpledb.backend.utils;

import java.security.SecureRandom;
import java.util.Random;

/**
 * @author Starry
 * @create 2022-12-24-6:10 PM
 * @Describe
 */
public class RandomUtil {

    /**
     * 返回一个指定长度的随机字节序列。
     * @param length
     * @return
     */
    public static byte[] randomBytes(int length) {
        Random r = new SecureRandom();
        byte[] result = new byte[length];
        r.nextBytes(result);
        return result;
    }
}
