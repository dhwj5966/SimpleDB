package top.wuzonghui.simpledb.backend.dm.page;

/**
 * @author Starry
 * @create 2022-12-24-2:21 PM
 * @Describe 以页为单位管理数据。
 */
public interface Page {
    void lock();
    void unlock();

    /**
     * 从内存中释放该页，调用管理该页的PageCache进行释放。
     */
    void release();
    void setDirty(boolean dirty);
    boolean isDirty();
    int getPageNumber();
    byte[] getData();
}

