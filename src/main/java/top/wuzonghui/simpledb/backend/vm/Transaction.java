package top.wuzonghui.simpledb.backend.vm;

import top.wuzonghui.simpledb.backend.tm.TransactionManager;
import top.wuzonghui.simpledb.backend.tm.TransactionManagerImpl;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Starry
 * @create 2022-12-31-6:23 PM
 * @Describe 事务的抽象。
 */
public class Transaction {
    /**
     * 事务的xid。
     */
    public long xid;

    /**
     * 事务的隔离级别。
     */
    public int level;

    /**
     * 快照字段，在事务新建的时候存储所有在事务新建的时候活跃的事务。
     */
    public Map<Long, Boolean> snapshot;

    public Exception error;

    public boolean autoAborted;

    public static final int READCOMMITTED = 0;
    public static final int REPEATABLEREAD = 1;


    /**
     * 创建一个Transaction对象，并返回。
     * @param xid 事务的xid
     * @param level 事务的隔离级别，目前支持READCOMMITTED和REPEATABLEREAD。
     * @param active 当前活跃的事务，以Map形式存储，key为事务的xid，value为事务对象。
     * @return 新建的事务对象，如果隔离级别为REPEATABLEREAD，则会把现在所有还在活跃的事务，保存到Transaction对象的snapshot字段中。
     */
    public static Transaction newTransaction(long xid, int level, Map<Long, Transaction> active) {
        Transaction transaction = new Transaction();
        transaction.xid = xid;
        transaction.level = level;
        //如果该事务的隔离级别是可重复读
        if (level == REPEATABLEREAD) {
            transaction.snapshot = new HashMap<>();
            //transaction.snapshot记录了所有现在还在活跃的事务。
            for (Long x : active.keySet()) {
                transaction.snapshot.put(x, true);
            }
        }
        return transaction;
    }

    /**
     * xid是否在该事务的快照中。事务对象初始化的时候，快照会保存所有在事务初始化时还在活跃的事务。
     * @param xid
     * @return true:在快照中。false：不在快照中。
     */
    public boolean isInSnapshot(long xid) {
        //超级xid默认不在快照中
        if (xid == TransactionManagerImpl.SUPER_XID) {
            return false;
        }
        return snapshot.containsKey(xid);
    }
}
