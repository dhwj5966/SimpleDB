package top.wuzonghui.simpledb.backend.dm.page;

import top.wuzonghui.simpledb.backend.dm.pagecache.PageCache;
import top.wuzonghui.simpledb.backend.utils.RandomUtil;

import java.util.Arrays;

/**
 * @author Starry
 * @create 2022-12-24-6:06 PM
 * @Describe 该类是用来特殊管理第一页的工具类。第一页用来Valid Check。
 * db启动的时候在100-107字节处填入一个随机字节，db关闭的时候将这个随机字节拷贝到108-115字节处。
 * 每次db启动需要通过Valid Check判断上一次是否是合法关闭，如果不是合法关闭就走数据恢复流程。
 */
public class PageOne {
    private static final int OF_VC = 100;
    private static final int LEN_VC = 8;

    /**
     * 初始化一个长度为8K的数组，并将该数组的100-107位设为随机。
     * @return
     */
    public static byte[] InitRaw() {
        byte[] result = new byte[PageCache.PAGE_SIZE];
        setVcOpen(result);
        return result;
    }

    /**
     * 设置Valid Check的数据库开启部分。
     * 1.将数据页设置为脏页
     * 2.将数据页的数据数组的100-107位，设置为随机字节序列。
     * @param pg
     */
    public static void setVcOpen(Page pg) {
        pg.setDirty(true);
        setVcOpen(pg.getData());
    }

    /**
     * 调用System.arraycopy()方法，将随机生成的长度为8的字节数组，复制到raw数组的100-107位。
     * @param raw
     */
    private static void setVcOpen(byte[] raw) {
        System.arraycopy(RandomUtil.randomBytes(LEN_VC), 0, raw, OF_VC, LEN_VC);
    }

    /**
     * 设置Valid Check的数据库关闭部分。
     * 1.将数据页设置为脏页
     * 2.将数据页的数据数组的100-107位，复制到自己的108-115位。
     * @param pg
     */
    public static void setVcClose(Page pg) {
        pg.setDirty(true);
        setVcClose(pg.getData());
    }

    /**
     * 调用System.arraycopy()方法，将raw数组的100-107位，复制到自己的108-115位。
     * @param raw
     */
    private static void setVcClose(byte[] raw) {
        System.arraycopy(raw, OF_VC, raw, OF_VC + LEN_VC, LEN_VC);
    }


    /**
     * 检查pg对象的数据，判断该数组的100-107位和 108-115位的数据是否一致。
     * @param pg
     * @return
     */
    public static boolean checkVc(Page pg) {
        return checkVc(pg.getData());
    }

    /**
     * 查看数组raw的100-107位和 108-115位的数据是否一致。
     * @param raw
     * @return
     */
    private static boolean checkVc(byte[] raw) {
        return Arrays.equals(raw, OF_VC, OF_VC + LEN_VC + 1, raw, OF_VC + LEN_VC, OF_VC + LEN_VC * 2 + 1);
    }



}
