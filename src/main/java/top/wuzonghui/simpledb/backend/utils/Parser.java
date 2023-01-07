package top.wuzonghui.simpledb.backend.utils;

import com.google.common.primitives.Bytes;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * @author Starry
 * @create 2022-12-22-7:22 PM
 * @Describe
 */
public class Parser {
    /**
     * 获取字节数组的前4位代表的int数值
     * @param buffer
     * @return
     */
    public static int parseInt(byte[] buffer) {
        return ByteBuffer.wrap(buffer, 0, 4).getInt();
    }



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

    /**
     * 读取buf数组的前2个字节，并返回前两个字节的short值
     * @param buf
     * @return
     */
    public static short parseShort(byte[] buf) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(buf, 0, 2);
        return byteBuffer.getShort();
    }

    /**
     * 将指定的value转换成长度为2的byte数组
     * @param value
     * @return
     */
    public static byte[] short2Byte(short value) {
        return ByteBuffer.allocate(2).putShort(value).array();
    }

    /**
     * 将指定的value转换成长度为4的byte数组
     * @param value
     * @return
     */
    public static byte[] int2Byte(int value) {
        return ByteBuffer.allocate(4).putInt(value).array();
    }

    /**
     * 解析字符串。
     * @param raw 待解析的字节数组。
     * @return ParseStringRes对象，包括String和next，next指明了下一个string的offset。
     */
    public static ParseStringRes parseString(byte[] raw) {
        int stringLength = parseInt(Arrays.copyOfRange(raw, 0, 4));
        String s = new String(Arrays.copyOfRange(raw, 4, 4 + stringLength));
        return new ParseStringRes(s, stringLength + 4);
    }

    /**
     * 根据str生成字节数组。格式为[strLength,4byte][str.getBytes()]
     * @param str 待生成数据的字符串。
     * @return 根据str生成的字节数组。
     */
    public static byte[] string2Byte(String str) {
        byte[] l = int2Byte(str.length());
        return Bytes.concat(l, str.getBytes());
    }

    /**
     * @Describe 将string类型转换为long类型。
     */
    public static long str2Uid(String key) {
        long seed = 13331;
        long res = 0;
        for(byte b : key.getBytes()) {
            res = res * seed + (long)b;
        }
        return res;
    }


}
