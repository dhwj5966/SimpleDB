package top.wuzonghui.simpledb.backend.dm.page;

import top.wuzonghui.simpledb.backend.dm.pagecache.PageCache;
import top.wuzonghui.simpledb.backend.utils.Parser;

import java.util.Arrays;

/**
 * @author Starry
 * @create 2022-12-24-8:14 PM
 * @Describe 操作普通数据页的工具类，SDB的普通数据页的前2个字节，用来存储FreeSpaceOffset(FSO)。
 * 这2个字节的FSO代表该数据页空闲位置的offset。
 */
public class PageX {
    private static final short OF_FREE = 0;
    private static final short OF_DATA = 2;
    public static final int MAX_FREE_SPACE = PageCache.PAGE_SIZE - OF_DATA;

    /**
     * 创建一个长度为8K的byte数组，并且将其前两个字节设置为2。
     * @return 创建的byte数组
     */
    public static byte[] initRaw() {
        byte[] result = new byte[PageCache.PAGE_SIZE];
        setFSO(result, OF_DATA);
        return result;
    }

    /**
     * 将指定的偏移量ofData设置到raw数组的前2个字节中。
     * @param raw
     * @param ofData 偏移量
     */
    private static void setFSO(byte[] raw, short ofData) {
        System.arraycopy(Parser.short2Byte(ofData), 0, raw, OF_FREE, OF_DATA);
    }

    /**
     * 获取Page对象的FSO
     * @param pg
     * @return
     */
    public static short getFSO(Page pg) {
        return getFSO(pg.getData());
    }



    //解析raw数组的前2个字节，并得出偏移量
    private static short getFSO(byte[] raw) {
        return Parser.parseShort(Arrays.copyOfRange(raw, 0, 2));
    }

    /**
     * 将byte数组raw中的数据，插入pg中，返回数据插入的位置。
     * 比如原本该page的offset为1026，传入一个长度为20的raw数组，
     * 则将raw数组的数据追加到page的[1026,1045]中，并且返回1026。
     * 具体步骤：
     * 1.将该页设置为脏页
     * 2.获取该页现在的offset
     * 3.调用System.arraycopy()方法，将raw数组的数据追加到页的数据区
     * 4.修改页的FSO
     * @param pg
     * @param raw
     * @return
     */
    public static short insert(Page pg, byte[] raw) {
        pg.setDirty(true);
        short offset = getFSO(pg);
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
        setFSO(pg.getData(), (short) (offset + raw.length));
        return offset;
    }

    /**
     * 获取数据页空闲空间的大小。
     * 数据页空闲空间的大小 = 页的总大小 - FSO
     * @param pg
     * @return
     */
    public static int getFreeSpace(Page pg) {
        return PageCache.PAGE_SIZE - (int) getFSO(pg);
    }

    /**
     * 该方法在数据库崩溃后重新打开时，恢复例程直接插入数据到页。
     * 将raw插入pg中的offset位置，并将pg的offset设置为较大的offset。
     * @param pg
     * @param raw
     * @param offset
     */
    public static void recoverInsert(Page pg, byte[] raw, short offset) {
        pg.setDirty(true);
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);

        short rawFSO = getFSO(pg.getData());
        if(rawFSO < offset + raw.length) {
            setFSO(pg.getData(), (short)(offset + raw.length));
        }
    }

    /**
     * 该方法在数据库崩溃后重新打开时，恢复例程直接修改页的数据。
     * 将raw插入pg中的offset位置，不更新FSO
     * @param pg
     * @param raw
     * @param offset
     */
    public static void recoverUpdate(Page pg, byte[] raw, short offset) {
        pg.setDirty(true);
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
    }
}
