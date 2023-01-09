package top.wuzonghui.simpledb.backend.dm.page;

import top.wuzonghui.simpledb.backend.dm.pagecache.PageCache;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Starry
 * @create 2022-12-24-2:23 PM
 * @Describe SDB默认的Page实现类。
 */
public class PageImpl implements Page{
    /**
     * 页的页号，从1开始
     */
    private int pageNumber;

    /**
     * 页实际存储的数据
     */
    private byte[] data;

    /**
     * 记录当前页面是否是脏页
     */
    private boolean dirty;

    /**
     * 页锁
     */
    private Lock lock;

    /**
     * 当前Page所处的缓冲区。
     */
    private PageCache pc;

    /**
     * PageImpl的构造方法，需要传入Page的页号，实际包含的数据，以及当前Page所处的缓冲区
     */
    public PageImpl(int pageNumber, byte[] data, PageCache pc) {
        this.pageNumber = pageNumber;
        this.data = data;
        this.pc = pc;
        this.lock = new ReentrantLock();
    }

    @Override
    public int getPageNumber() {
        return pageNumber;
    }

    @Override
    public byte[] getData() {
        return data;
    }


    @Override
    public boolean isDirty() {
        return dirty;
    }

    @Override
    public void lock() {
        lock.lock();
    }

    @Override
    public void unlock() {
        lock.unlock();
    }

    @Override
    public void release() {
        pc.release(this);
    }

    @Override
    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

}
