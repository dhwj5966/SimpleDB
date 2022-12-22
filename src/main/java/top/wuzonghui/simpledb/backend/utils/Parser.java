package top.wuzonghui.simpledb.backend.utils;

import java.nio.ByteBuffer;

/**
 * @author Starry
 * @create 2022-12-22-7:22 PM
 * @Describe
 */
public class Parser {
    /**
     * 获取字节数组代表的long数值
     * @param buf
     * @return
     */
    public static long parseLong(byte[] buf) {
        ByteBuffer wrap = ByteBuffer.wrap(buf, 0, 8);
        return wrap.getLong();
    }

    /**
     * 根据传入的value，返回一个byte数组
     * @param value
     * @return
     */
    public static byte[] long2Byte(long value) {
        return ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(value).array();
    }
}
