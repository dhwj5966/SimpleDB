package top.wuzonghui.simpledb.backend.dm.dataitem;

import top.wuzonghui.simpledb.backend.common.SubArray;
import top.wuzonghui.simpledb.backend.dm.DataManagerImpl;
import top.wuzonghui.simpledb.backend.dm.page.Page;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author Starry
 * @create 2022-12-28-5:23 PM
 * @Describe
 */
public class DataItemImpl implements DataItem{

    /**
     * DataItem的数据部分，包括[ValidFlag,1byte],[DataSize,2byte],[Data]。
     * 封装成SubArray的目的是让多个DataItem可以操作同一个byte[]的不同部分。
     */
    private SubArray raw;

    /**
     * 一个临时数组，在修改数据的时候，将旧数据存储到临时数组里，以便撤销
     */
    private byte[] oldRaw;

    /**
     * 操作该DataItem的DataManager
     */
    private DataManagerImpl dm;

    /**
     * 该DataItem的id，由pageno和offset组成，可以通过uid解析出pageno和offset，
     * 定位到数据实际存储的page(pageno)以及数据在page中存储的位置(offset)。
     */
    private long uid;

    /**
     * 存储当前DataItem的Page
     */
    private Page pg;

    /**
     * readLock
     */
    private Lock rLock;

    /**
     * writeLock
     */
    private Lock wLock;

    /**
     * Valid区的offset
     */
    static final int OF_VALID = 0;

    /**
     * Size区的offset
     */
    static final int OF_SIZE = 1;

    /**
     * Data区的offset
     */
    static final int OF_DATA = 3;

    public DataItemImpl(SubArray raw, byte[] oldRaw, Page pg, long uid, DataManagerImpl dm) {
        this.raw = raw;
        this.oldRaw = oldRaw;
        ReadWriteLock lock = new ReentrantReadWriteLock();
        rLock = lock.readLock();
        wLock = lock.writeLock();
        this.dm = dm;
        this.uid = uid;
        this.pg = pg;
    }

    @Override
    public SubArray data() {
        return new SubArray(raw.raw, raw.start + OF_DATA, raw.end);
    }

    @Override
    public void before() {
        wLock.lock();
        pg.setDirty(true);
        System.arraycopy(raw.raw, raw.start, oldRaw, 0, oldRaw.length);
    }

    @Override
    public void unBefore() {
        System.arraycopy(oldRaw, 0, raw.raw, raw.start, oldRaw.length);
        wLock.unlock();
    }

    @Override
    public void after(long xid) {
        dm.logDataItem(xid, this);
        wLock.unlock();
    }



    @Override
    public void release() {
        dm.releaseDataItem(this);
    }

    @Override
    public void lock() {
        wLock.lock();
    }

    @Override
    public void unlock() {
        wLock.unlock();
    }

    @Override
    public void rLock() {
        rLock.lock();
    }

    @Override
    public void rUnLock() {
        rLock.unlock();
    }

    @Override
    public Page page() {
        return pg;
    }

    @Override
    public long getUid() {
        return uid;
    }

    @Override
    public byte[] getOldRaw() {
        return oldRaw;
    }

    @Override
    public SubArray getRaw() {
        return raw;
    }

    /**
     * 判断该DataItem对象是否被逻辑删除。
     * @return true：当前DataItem对象没有被逻辑删除。 false：当前DataItem对象已经被逻辑删除。
     */
    public boolean isValid() {
        return raw.raw[raw.start + OF_VALID] == (byte)0;
    }
}
