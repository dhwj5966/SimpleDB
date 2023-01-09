package top.wuzonghui.simpledb.backend.vm;

import com.google.common.primitives.Bytes;
import top.wuzonghui.simpledb.backend.common.SubArray;
import top.wuzonghui.simpledb.backend.dm.DataManager;
import top.wuzonghui.simpledb.backend.dm.dataitem.DataItem;
import top.wuzonghui.simpledb.backend.utils.Parser;

import javax.xml.crypto.Data;
import java.util.Arrays;

/**
 * @author Starry
 * @create 2022-12-30-7:13 PM
 * @Describe 通过DM模块可以操作DataItem，VM则管理所有的DataItem，向上层提供Entry。
 * 上层通过VM模块操作数据的最小单位就是Entry。
 * 每个记录有多个版本(Version),当上层模块对Entry进行修改时，VM就会为这个Entry创建一个新的Version。
 * 此外该类对外提供一些工具方法。
 * @Detail 格式：[XMIN, 8byte] [XMAX, 8byte] [data]。
 * XMIN 是创建该条记录（版本）的事务编号。
 * XMAX 则是删除该条记录（版本）的事务编号。
 * DATA 就是这条记录持有的数据。
 */
public class Entry {
    /**
     * Entry中XMIN部分的offset
     */
    private static final int OF_XMIN = 0;

    /**
     * Entry中XMAX部分的offset
     */
    private static final int OF_XMAX = OF_XMIN+8;

    /**
     * Entry中data部分的offset
     */
    private static final int OF_DATA = OF_XMAX+8;

    /**
     * Entry的uid。和Entry所在的dataItem的uid一致。
     */
    private long uid;

    /**
     * 一条Entry存储在一条DataItem中，所以需要持有该DataItem的引用。
     */
    private DataItem dataItem;

    /**
     * 操作该Entry对象的VersionManager对象
     */
    private VersionManager vm;

    /**
     * 创建一个新的Entry对象
     * @param vm 操作该Entry对象的VersionManager对象
     * @param dataItem 该Entry所处的dataItem对象
     * @param uid 该Entry的uid
     * @return 创建的Entry对象
     */
    public static Entry newEntry(VersionManager vm, DataItem dataItem, long uid) {
        Entry entry = new Entry();
        entry.vm = vm;
        entry.dataItem = dataItem;
        entry.uid = uid;
        return entry;
    }

    /**
     * 封装符合[XMIN, 8byte] [XMAX, 8byte] [data]格式的byte数组。
     * @param xid 创建该条记录的事务的xid
     * @param data Entry的数据部分
     * @return
     */
    public static byte[] wrapEntryRaw(long xid, byte[] data) {
        byte[] XMIN = Parser.long2Byte(xid);
        byte[] XMAX = new byte[OF_DATA - OF_XMAX];
        return Bytes.concat(XMIN, XMAX, data);
    }

    /**
     * 返回该条Entry的data部分。
     * @return 以拷贝byte数组的形式返回。
     */
    public byte[] data() {
        //由于涉及到对DataItem的读操作，因此加读锁
        dataItem.rLock();
        try {
            SubArray data = dataItem.data();
            byte[] result = new byte[data.end - data.start - OF_DATA];
            System.arraycopy(data.raw, data.start + OF_DATA, result, 0, result.length);
            return result;
        } finally {
            dataItem.rUnLock();
        }
    }

    /**
     * 设置该Entry的XMAX部分
     * @param xid 删除该Entry的事务的xid。
     */
    public void setXmax(long xid) {
        //由于before和after就包含了加锁解锁的操作了，因此不需要再专门加锁加锁
        dataItem.before();
        try {
            SubArray data = dataItem.data();
            System.arraycopy(Parser.long2Byte(xid), 0, data.raw, data.start + OF_XMAX, 8);
        } finally {
            dataItem.after(xid);
        }
    }

    /**
     * 释放该条Entry，由VM实现。
     */
    public void release() {
        ((VersionManagerImpl)vm).releaseEntry(this);
    }

    /**
     * @Describe 删除该条Entry。
     * @Detail 在这里顺便总结一下各层释放的逻辑。
     * 1.删除该条Entry，就是释放该条Entry依赖的DataItem对象。
     * 2.释放DataItem对象，即释放DataItem对象所在的page。
     * 3.释放Page，则是将Page从缓存中删除，然后将Page数据从内存中刷到文件系统中。
     * 4.注意：DataItem和Page的缓存都是用的引用计数缓存，并不是调用release()方法就一定会释放，
     * 每一次get()会让该对象的引用次数+1,release()方法则会让该对象的引用次数-1，只有引用次数到0了才会真的走释放逻辑。
     */
    public void remove(){
        dataItem.release();
    }

    /**
     * 获取该Entry的XMIN部分。
     * @return XMIN代表生成该条Entry的事务编号。
     */
    public long getXmin() {
        dataItem.rLock();
        try {
            SubArray subArray = dataItem.data();
            byte[] bytes = Arrays.copyOfRange(subArray.raw, subArray.start + OF_XMIN, subArray.start + OF_XMAX);
            return Parser.parseLong(bytes);
        } finally {
            dataItem.rUnLock();
        }
    }

    /**
     * 获取该条Entry的XMAX部分。
     * @return XMAX代表删除该条Entry的事务编号。如果是0表示该记录没有被删除。
     */
    public long getXmax() {
        dataItem.rLock();
        try {
            SubArray subArray = dataItem.data();
            byte[] bytes = Arrays.copyOfRange(subArray.raw, subArray.start + OF_XMAX, subArray.start + OF_DATA);
            return Parser.parseLong(bytes);
        } finally {
            dataItem.rUnLock();
        }
    }

    public long getUid() {
        return uid;
    }


    /**
     * 根据VersionManager对象，以及uid，加载一个新的Entry。
     * @param versionManager 管理Entry的VersionManager对象
     * @param uid Entry的uid。根据该uid，可以解析出Entry所处DataItem的位置(pageno、offset)。
     * @return Entry对象。
     * @throws Exception
     */
    public static Entry loadEntry(VersionManager versionManager, long uid) throws Exception {
        //将vm对象强转成VersionManagerImpl对象。本数据库的vm对象皆是VersionManagerImpl对象，因此可以强转。
        VersionManagerImpl versionManagerImpl = (VersionManagerImpl) versionManager;
        //拿到VersionManagerImpl对象的dataManager字段。
        DataManager dataManager = versionManagerImpl.dataManager;
        //通过DataManager和uid读出Entry所在的dataItem。
        DataItem dataItem = dataManager.read(uid);
        return newEntry(versionManager, dataItem, uid);
    }
}
