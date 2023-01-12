package top.wuzonghui.simpledb.backend.vm;

import top.wuzonghui.simpledb.backend.tm.TransactionManager;

/**
 * @author Starry
 * @create 2022-12-31-6:22 PM
 * @Describe 工具类，用来判断Transaction和Entry的可见性关系。
 * @Detail
 */
public class Visibility {

    /**
     * 一个记录e对于事务t来说是否是版本跳跃。
     * @param tm 管理事务t的TransactionManager。
     * @param t 事务t。
     * @param e 记录e。
     * @return true：是版本跳跃。false：不是版本跳跃
     * @Detail
     * 1.可重复读隔离级别可能会导致版本跳跃问题。
     * 什么是版本跳跃问题？在可重复读隔离级别下，一个事务T1开启后，T1读出数据x的值为0，
     * 一个T1不可见的事务T2修改了x的值为1(修改操作会生成一个新的Version)。
     * 此时T1如果更新x的值到2，虽然可能做到但是跳过了一个Version。
     * 这就是发生了版本跳跃。
     * 2.可重复读隔离级别下，事务T1不可见的事务T2有：
     * 在T1开启后才开启的事务 和 在T1的快照中的事务。
     * 3.因此检查记录e对事务t来说是否是版本跳跃，可以取出要修改的数据X的最新提交版本，并检查该最新版本的创建者对当前事务是否可见。
     * 4.解决版本跳跃的方法：如果T1需要修改X，而X已经被T1不可见的事务T2修改过了，那么要求T1回滚。
     */
    public static boolean isVersionSkip(TransactionManager tm, Transaction t, Entry e) {
        //READCOMMITTED隔离级别不存在版本跳跃问题。
        if (t.level == Transaction.READCOMMITTED) return false;
        //xmax是删除该条记录的事务的xid。
        long xmax = e.getXmax();
        //注意要判断xmax是否提交，因为事务t只会被已提交的事务所影响。
        return tm.isCommitted(xmax) && (xmax > t.xid || t.isInSnapshot(xmax));
    }

    /**
     * 记录e对事务t是否可见。
     * @param tm 管理事务t的TransactionManager。
     * @param t 事务t。
     * @param e 记录e。
     * @return true:可见。false：不可见。
     */
    public static boolean isVisible(TransactionManager tm, Transaction t, Entry e) {
        if (e == null || e.dataItemIsNull()) {
            return false;
        }
        if (t.level == Transaction.READCOMMITTED) {
            return readCommitted(tm, t, e);
        } else {
            return repeatableRead(tm, t, e);
        }
    }

    /**
     * readCommitted隔离级别下，记录e对事务t是否可见。
     * @param tm 管理事务t的TransactionManager对象。
     * @param t 事务t
     * @param e 记录e
     * @return true：可见。false：不可见
     * @Detail 对于一个事务T，版本E是否对T可见的逻辑如下(Read Committed版本)：
     * 1.版本E由T创建，且未被删除。
     * 2.版本E不是由T创建，但是创建E的事务T2已提交，且E未被删除。
     * 3.版本E已被删除，但是由一个未提交的事务T3删除。
     */
    private static boolean readCommitted(TransactionManager tm, Transaction t, Entry e) {
        long xid = t.xid;
        long xmin = e.getXmin();
        long xmax = e.getXmax();
        //1.版本E由T创建，且未被删除。
        if (xmin == xid && xmax == 0) {
            return true;
        }
        //2.版本E不是由T创建，但是创建E的事务T2已提交，且E未被删除。
        if (xmax == 0 && tm.isCommitted(xmin)) {
            return true;
        }

        //3.版本E已被删除，但是是由一个未提交的事务T3删除。
        if (tm.isCommitted(xmin) && xmax != xid && !tm.isCommitted(xmax)) {
            return true;
        }
        return false;
    }

    /**
     * repeatableRead隔离级别下，记录e对事务t是否可见。
     * @param tm 管理事务t的TransactionManager对象。
     * @param t 事务t
     * @param e 记录e
     * @return true：可见。false：不可见
     * @Detail 事务只能读取它开始时, 就已经结束的那些事务产生的数据版本。
     * 进一步归纳，即事务t需要忽略 从本事务后开始的事务的数据，本事务开始的时候还是active的事务的数据。
     * 1.版本E由T创建，且未删除。
     * 2.由一个已提交的事务T2创建，
     * 且T2的xid小于T的xid，
     * 且T2在T开始前提交，
     * 且(记录不是被删除的状态 或 (记录被删除但由其他事务T3删除, T3尚未提交 或 T3在T开始后才开始 或 T3在T开始前还在活跃))
     */
    private static boolean repeatableRead(TransactionManager tm, Transaction t, Entry e) {
        long xid = t.xid;
        //创建该记录的事务的xid
        long xmin = e.getXmin();
        //删除该记录的事务的xid
        long xmax = e.getXmax();
        //1.版本E由T创建，且未删除
        if(xmin == xid && xmax == 0) return true;

        if (tm.isCommitted(xmin) && xmin < xid && !t.isInSnapshot(xmin)) {
            if (xmax == 0) {
                return true;
            }
            if (xmax != xid && (!tm.isCommitted(xmax) || xmax > xid || t.isInSnapshot(xmax))) {
                return true;
            }
        }
        return false;
    }

}
