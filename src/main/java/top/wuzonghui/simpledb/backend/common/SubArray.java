package top.wuzonghui.simpledb.backend.common;

/**
 * @author Starry
 * @create 2022-12-24-1:52 PM
 * @Describe 一个工具类,该类的作用是让使用者只能操作数组的部分元素。
 * 比如new SubArray(buffer, 0, 10),则该类记录了该对象能操作buffer这个byte数组的索引范围是[start,end]，
 * 当然，具体的限制逻辑要在代码中完成,该类并没有限制用户操作，只是对start和end进行了记录。
 */
public class SubArray {
    public byte[] raw;
    public int start;
    public int end;

    public SubArray(byte[] raw, int start, int end) {
        this.raw = raw;
        this.start = start;
        this.end = end;
    }
}
