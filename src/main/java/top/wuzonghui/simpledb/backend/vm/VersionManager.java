package top.wuzonghui.simpledb.backend.vm;

import top.wuzonghui.simpledb.backend.dm.DataManager;
import top.wuzonghui.simpledb.backend.tm.TransactionManager;

/**
 * @author Starry
 * @create 2022-12-30-7:20 PM
 * @Describe VM层通过VersionManager接口，向上层提供功能。
 */
public interface VersionManager {
    /**
     * @Describe 指定的事务read数据，读出记录的data部分。
     * @param xid 指定的事务的xid。
     * @param uid 要读取的数据所在Entry的uid。
     * @return 读取到的Entry的data部分。如果Entry为null或Entry对事务不可见，则返回null。
     * @throws Exception 如果该事务的Error字段不为null，会抛出该异常。
     */
    byte[] read(long xid, long uid) throws Exception;

    /**
     * @Describe 指定的事务insert数据。
     * @param xid 插入记录的事务的xid。
     * @param data 要插入的数据，会被封装成Entry的格式并插入。
     * @return 实际插入到Page-DataItem的uid，通过该uid可以获取到DataItem。
     * @throws Exception
     */
    long insert(long xid, byte[] data) throws Exception;

    /**
     * @Describe 由xid将指定uid的记录删除。
     * @param xid 用来指定事务。
     * @param uid 用来指定记录。
     * @return 是否成功删除。true：成功删除。false：删除失败。
     * @throws Exception
     */
    boolean delete(long xid, long uid) throws Exception;

    /**
     * @Describe 开启一个事务。
     * @param level 该事务的隔离级别。
     * @return 新开启的事务的xid。
     */
    long begin(int level);

    /**
     * @Describe 提交一个事务。
     * @param xid 提交事务的xid。
     * @throws Exception 如果该事务对应的事务对象的Error对象不为空，抛出该异常。
     */
    void commit(long xid) throws Exception;

    /**
     * @Describe 手动提交一个事务。
     * @param xid 提交的事务的xid。
     */
    void abort(long xid);

    public static VersionManager newVersionManager(TransactionManager tm, DataManager dm) {
        return new VersionManagerImpl(tm, dm);
    }
}
