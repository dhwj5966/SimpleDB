package top.wuzonghui.simpledb.backend.tm;

/**
 * @author Starry
 * @create 2022-12-22-5:04 PM
 * @Describe 通过TM可以查询一个事务的状态，TM通过XID文件实现对事务状态的记录。
 * 事务的 XID 从 1 开始标号，并自增，不可重复。
 * 特殊规定xid为0的事务是超级事务，当一些操作想在不申请事务的情况下进行，就可以将操作的xid设为0，xid为0的事务的状态永远是commited。
 */
public interface TransactionManager {
    //开启事务
    long begin();
    //提交事务
    void commit(long xid);
    //取消一个事务
    void abort(long xid);
    //查询一个事务的状态是否是active
    boolean isActive(long xid);
    //查询一个事务是否已提交
    boolean isCommited(long xid);
    //查询一个事务是否已取消
    boolean isAborted(long xid);
    //关闭TM
    void close();
}
