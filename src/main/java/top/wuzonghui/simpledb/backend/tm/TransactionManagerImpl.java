package top.wuzonghui.simpledb.backend.tm;

import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;

/**
 * @author Starry
 * @create 2022-12-22-5:08 PM
 * @Describe 如何实现事件管理器？维护一个xid文件，该文件前8个字节用来记录该文件管理的事务的数量。
 * 然后对于管理的每个事务，用1个字节来记录事务的状态。一个事务的状态就存储在该事务的(xid + 7)的位置。
 *
 */
public class TransactionManagerImpl implements TransactionManager{

    //XID文件头长度
    static final int LEN_XID_HEADER_LENGTH = 8;
    // 每个事务的占用长度
    private static final int XID_FIELD_SIZE = 1;

    // 事务的三种状态
    private static final byte FIELD_TRAN_ACTIVE   = 0;
    private static final byte FIELD_TRAN_COMMITTED = 1;
    private static final byte FIELD_TRAN_ABORTED  = 2;

    // 超级事务，永远为commited状态
    public static final long SUPER_XID = 0;

    static final String XID_SUFFIX = ".xid";


    private RandomAccessFile file;

    private FileChannel fc;

    private long xidCounter;

    private Lock counterLock;

    /**
     * 实例化时调用，对xid文件进行校验，确保这是一个合法的xid文件，主要是对xid文件的长度进行校验
     */
    private void checkXIDCounter() {

    }

    @Override
    public long begin() {

        return 0;
    }

    @Override
    public void commit(long xid) {

    }

    @Override
    public void abort(long xid) {

    }

    @Override
    public boolean isActive(long xid) {
        return false;
    }

    @Override
    public boolean isCommited(long xid) {
        return false;
    }

    @Override
    public boolean isAborted(long xid) {
        return false;
    }

    @Override
    public void close() {

    }
}
