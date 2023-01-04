package top.wuzonghui.simpledb.backend.tm;

import top.wuzonghui.simpledb.backend.utils.Panic;
import top.wuzonghui.simpledb.backend.utils.Parser;
import top.wuzonghui.simpledb.common.Error;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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

    /**
     * xid文件映射
     */
    private RandomAccessFile file;

    /**
     * file文件的通道
     */
    private FileChannel fc;

    /**
     * 当前事务管理器管理的事务的数量
     */
    private long xidCounter;

    private Lock counterLock;

    public TransactionManagerImpl(RandomAccessFile file, FileChannel fc) {
        this.file = file;
        this.fc = fc;
        counterLock = new ReentrantLock();
        checkXIDCounter();
    }

    /**
     * 实例化时调用，对xid文件进行校验，确保这是一个合法的xid文件，主要是对xid文件的长度进行校验
     */
    private void checkXIDCounter() {
        //获取文件的长度
        long fileLength = 0;
        try {
            fileLength = file.length();
        } catch (IOException e) {
            Panic.panic(Error.BadXIDFileException);
        }
        //如果文件长度小于8，直接报错吧
        if (fileLength < 8) {
            Panic.panic(Error.BadXIDFileException);
        }
        //分配8个字节的缓冲区空间
        ByteBuffer buffer = ByteBuffer.allocate(LEN_XID_HEADER_LENGTH);
        //确保指针在0处，读取8个字节的数据到buffer中
        try {
            fc.position(0);
            fc.read(buffer);
        } catch (Exception e) {
            Panic.panic(e);
        }
        //解析缓冲区的前8个字节组成的long数值，这里之所有要绕这一步，是因为文件头虽然这里是8字节，但为了便于扩展，还是有可能变的，因此不能直接读出long
        this.xidCounter = Parser.parseLong(buffer.array());
        long end = getXidPosition(this.xidCounter + 1);
        if(end != fileLength) {
            Panic.panic(Error.BadXIDFileException);
        }
    }


    /**
     * 根据事务xid取得其在xid文件中对应的字节位置，从0开始计数，比如xid为1，则为8，xid为10，则为17
     * @param xid
     * @return xid在文件中对应的位置
     */
    private long getXidPosition(long xid) {
        return LEN_XID_HEADER_LENGTH + (xid - 1) * XID_FIELD_SIZE;
    }



    @Override
    public long begin() {
        /*
            开启一个事务，就需要在xid文件后新增一个字节的记录，并且新开启事务的xid采用从1开始自增的分配策略。
            需要做的工作：
            1.xidCounter + 1,并且注意要修改xid文件的头部(存储事务数量的那8个字节)
            2.在文件末尾追加一字节
            索性封装成2个方法!
         */
        counterLock.lock();
        try {
            long xid = xidCounter + 1;
            incrXidCounter();
            updateFileByXIDAndStatus(xid, FIELD_TRAN_ACTIVE);
            return xid;
        } finally {
            counterLock.unlock();
        }
    }

    //
    private void incrXidCounter() {
        xidCounter++;
        //修改header头部文件
        ByteBuffer buffer = ByteBuffer.wrap(Parser.long2Byte(xidCounter));
        try {
            fc.position(0);
            fc.write(buffer);
        } catch (Exception e) {
            Panic.panic(e);
        }
        try {
            //强制将数据刷到硬盘中
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    /**
     * 将指定xid的事务的状态改为status，即将文件对应位置的数值修改为status。
     * @param xid
     * @param status
     */
    private void updateFileByXIDAndStatus(long xid, byte status) {
        //偏移量
        long offset = getXidPosition(xid);
        //1字节的数组
        byte[] buffer = new byte[XID_FIELD_SIZE];
        buffer[0] = status;
        try {
            fc.position(offset);
            fc.write(ByteBuffer.wrap(buffer));
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            //强制将数据刷到硬盘中
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    @Override
    public void commit(long xid) {
        /*
            提交指定事务需要将记录该事务的字节的数值修改
         */
        updateFileByXIDAndStatus(xid, FIELD_TRAN_COMMITTED);
    }

    @Override
    public void abort(long xid) {
        updateFileByXIDAndStatus(xid, FIELD_TRAN_ABORTED);
    }

    @Override
    public boolean isActive(long xid) {
        if(xid == SUPER_XID) return false;
        return checkXIDIsStatus(xid, FIELD_TRAN_ACTIVE);
    }

    @Override
    public boolean isCommitted(long xid) {
        if(xid == SUPER_XID) return true;
        return checkXIDIsStatus(xid, FIELD_TRAN_COMMITTED);
    }

    @Override
    public boolean isAborted(long xid) {
        if(xid == SUPER_XID) return true;
        return checkXIDIsStatus(xid, FIELD_TRAN_ABORTED);
    }

    //判断指定事务的状态是否和status相等
    private boolean checkXIDIsStatus(long xid, int status) {
        //获取偏移量
        long offset = getXidPosition(xid);
        //改变指针位置，并读取数据
        ByteBuffer buffer = ByteBuffer.allocate(1);
        try {
            fc.position(offset);
            fc.read(buffer);
        } catch (IOException e) {
            Panic.panic(e);
        }
        return buffer.array()[0] == status;
    }

    @Override
    public void close() {
        try {
            file.close();
            fc.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }
}
