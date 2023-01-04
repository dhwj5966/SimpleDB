package top.wuzonghui.simpledb.backend.vm;

import top.wuzonghui.simpledb.backend.common.AbstractCache;
import top.wuzonghui.simpledb.backend.dm.DataManager;
import top.wuzonghui.simpledb.backend.tm.TransactionManager;
import top.wuzonghui.simpledb.backend.tm.TransactionManagerImpl;
import top.wuzonghui.simpledb.backend.utils.Panic;
import top.wuzonghui.simpledb.common.Error;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Starry
 * @create 2023-01-02-7:30 PM
 * @Describe VersionManager接口的实现类
 */
public class VersionManagerImpl extends AbstractCache<Entry> implements VersionManager {
    /**
     * VM层实现功能依赖于TM层。
     */
    TransactionManager transactionManager;

    /**
     * VM层实现功能依赖于DM层。
     */
    DataManager dataManager;

    /**
     * 用一个Map，存储所有活跃的事务。{xid -> transaction}
     */
    Map<Long, Transaction> activeTransaction;

    /**
     * 锁
     */
    Lock lock;

    /**
     * 维护一张xid和uid的关系图，以进行死锁检测,当xid占用或释放uid的时候，需要调用相关方法维护关系图。
     */
    LockTable locktable;

    public VersionManagerImpl(TransactionManager transactionManager, DataManager dataManager) {
        //缓存数量：不限制。
        super(0);
        this.transactionManager = transactionManager;
        this.dataManager = dataManager;
        this.locktable = new LockTable();
        this.lock = new ReentrantLock();
        this.activeTransaction = new HashMap<>();
        //当前活跃的事务中，添加一条超级事务。超级事务始终处于活跃状态。
        activeTransaction.put(TransactionManagerImpl.SUPER_XID,
                Transaction.newTransaction(TransactionManagerImpl.SUPER_XID, 0, null));
    }

    @Override
    protected Entry getForCache(long uid) throws Exception {
        /*
            当Entry不在缓存中时，获取Entry的方法。
         */
        Entry entry = Entry.loadEntry(this, uid);
        if (entry == null) {
            throw Error.NullEntryException;
        }
        return entry;
    }

    @Override
    protected void releaseForCache(Entry entry) {
        entry.remove();
    }

    /**
     * 释放Entry。
     */
    public void releaseEntry(Entry entry) {
        //调用super.release(),release方法会去调releaseForCache，实际上又是调的entry.remove()
        super.release(entry.getUid());
    }


    @Override
    public long begin(int level) {
        lock.lock();
        try {
            //调用TM层的begin方法，获得一个底层根据文件系统分配的xid。
            long xid = transactionManager.begin();
            Transaction transaction = Transaction.newTransaction(xid, level, activeTransaction);
            activeTransaction.put(xid, transaction);
            return xid;
        } finally {
            lock.unlock();
        }
    }


    @Override
    public void commit(long xid) throws Exception {
        /*
            加锁地根据xid获取到事务对象。
            如果该事务运行过程中出错，报错。
            事务的活跃列表中移除xid。
            locktable、tm层提交事务。
         */
        lock.lock();
        Transaction transaction = activeTransaction.get(xid);
        lock.unlock();

        //疑问：这里为什么要catch异常。
        //当commit一个error字段不为null的事务的时候，捕获该异常，虚拟机停止。
        try {
            if (transaction.error != null) {
                throw transaction.error;
            }
        } catch (NullPointerException n) {
            System.out.println(xid);
            System.out.println(activeTransaction.keySet());
            Panic.panic(n);
        }

        lock.lock();
        activeTransaction.remove(xid);
        lock.unlock();

        locktable.remove(xid);
        transactionManager.commit(xid);
    }


    @Override
    public void abort(long xid) {
        internAbort(xid, false);
    }

    /**
     * 取消一个事务。
     *
     * @param xid         取消的事务的xid。
     * @param autoAborted 是否是自动提交。
     */
    private void internAbort(long xid, boolean autoAborted) {
        /*
            此处有2个疑问。
            1.设置t.Error、t.autoAborted字段的意义是什么？
            2.为什么当autoAborted为true的时候，就可以不移除activeTransaction中的key-value？
         */
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        if (!autoAborted) {
            activeTransaction.remove(xid);
        }
        lock.unlock();

        if (t.autoAborted) return;
        locktable.remove(xid);
        transactionManager.abort(xid);
    }


    @Override
    public byte[] read(long xid, long uid) throws Exception {
        //提示：获取事务要加锁，要判断事务是否出错，如果Entry为null返回null，如果事务对资源不可见也返回null。
        lock.lock();
        Transaction transaction = activeTransaction.get(xid);
        lock.unlock();

        if (transaction.error != null) {
            throw transaction.error;
        }
        Entry entry = null;
        try {
            entry = get(uid);
        } catch (Exception e) {
            //如果Entry为空，返回null。
            if (e == Error.NullEntryException) {
                return null;
            } else {
                throw e;
            }
        }
        try {
            if (Visibility.isVisible(transactionManager, transaction, entry)) {
                return entry.data();
            } else {
                return null;
            }
        } finally {
            entry.release();
        }
    }

    @Override
    public long insert(long xid, byte[] data) throws Exception {
        lock.lock();
        Transaction transaction = activeTransaction.get(xid);
        lock.unlock();

        if (transaction.error != null) {
            throw transaction.error;
        }

        byte[] d = Entry.wrapEntryRaw(xid, data);
        return dataManager.insert(xid, d);
    }

    @Override
    public boolean delete(long xid, long uid) throws Exception {
        //加锁获取事务，有错抛错。
        //根据uid获取到Entry。如果获取到空Entry，返回false。
        //不可见则不能删除，返回false。
        //xid尝试获取uid，为什么是获取？因为删除的本质就是获取到该uid，然后把xmax设为xid。如果获取报错了说明发生死锁，
        // 设置事务的error字段，internAbort(xid, true);设置事务自动提交，抛出异常。
        //如果xmax已经是xid了，返回false。
        //判断是否发生了版本跳跃，和发生了死锁的处理逻辑是一样的：
        // 1.设置Error字段2.调用internAbort(xid, true)3.设置autoAborted字段4.抛出异常。
        //如果发生了死锁或是版本跳跃，应该自动回滚事务。设置error字段意味着接下来该事务的所有操作都会报错。
        //调用internAbort方法，是自动回滚事务，将locktable中的边删除，调用tm的abort方法。
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if (t.error != null) {
            throw t.error;
        }

        //获取到Entry
        Entry entry = null;
        try {
            entry = super.get(uid);
        } catch (Exception e) {
            if (e == Error.NullEntryException) {
                return false;
            } else {
                throw e;
            }
        }
        try {
            //如果要事务对当前记录不可见，不可以删除
            if (!Visibility.isVisible(transactionManager, t, entry)) {
                return false;
            }
            //删除资源的时候也需要占用资源，查看是否会发生死锁。
            Lock l = null;
            try {
                l = locktable.add(xid, uid);
            } catch (Exception e) {
                //如果发生死锁了，设置事务的error字段。这里有一些疑问，为什么不直接让挂掉？
                t.error = Error.ConcurrentUpdateException;
                internAbort(xid, true);
                t.autoAborted = true;
                throw t.error;
            }
            //如果不是死锁，那么就等待该线程被解锁。这里是不是写错了，因为lock是可重入锁？是否应该用阻塞队列之类的来实现。
            if (l != null) {
                l.lock();
                l.unlock();
            }
            //如果该记录已经被该xid删除
            if (entry.getXmax() == xid) {
                return false;
            }
            //如果是版本跳跃
            if(Visibility.isVersionSkip(transactionManager, t, entry)) {
                t.error = Error.ConcurrentUpdateException;
                internAbort(xid, true);
                t.autoAborted = true;
                throw t.error;
            }

            entry.setXmax(xid);
            return true;
        } finally {
            entry.release();
        }
    }

}
