package top.wuzonghui.simpledb.backend.tm;

import top.wuzonghui.simpledb.backend.utils.Panic;
import top.wuzonghui.simpledb.common.Error;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author Starry
 * @create 2022-12-22-5:04 PM
 * @Describe 通过TM可以查询一个事务的状态，TM通过XID文件实现对事务状态的记录。
 * 事务的 XID 从 1 开始标号，并自增，不可重复。
 * 特殊规定xid为0的事务是超级事务，当一些操作想在不申请事务的情况下进行，就可以将操作的xid设为0，xid为0的事务的状态永远是commited。
 */
public interface TransactionManager {
    /**
     * 开启一个事务，该过程上锁
     * @return 返回新开启事务的xid
     */
    long begin();

    /**
     * 提交指定事务
     * @param xid
     */
    void commit(long xid);

    /**
     * 取消指定事务
     * @param xid
     */
    void abort(long xid);

    //查询一个事务的状态是否是active
    boolean isActive(long xid);
    //查询一个事务是否已提交
    boolean isCommitted(long xid);
    //查询一个事务是否已取消
    boolean isAborted(long xid);
    //关闭TM
    void close();

    /**
     * 根据指定路径，创建一个xid文件,并创建TransactionManager对象
     * @param path
     * @return
     */
    public static TransactionManager create(String path) {
        File f = new File(path + TransactionManagerImpl.XID_SUFFIX);
        try {
            //无法创建则报错
            if(!f.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        } catch (Exception e) {
            Panic.panic(e);
        }
        //无法读写则报错
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        // 写空XID文件头
        ByteBuffer buf = ByteBuffer.wrap(new byte[TransactionManagerImpl.LEN_XID_HEADER_LENGTH]);
        try {
            fc.position(0);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        return new TransactionManagerImpl(raf, fc);
    }

    /**
     * 从一个已有的xid文件创建TM
     * @param path
     * @return
     */
    public static TransactionManager open(String path) {
        File f = new File(path+TransactionManagerImpl.XID_SUFFIX);
        //文件不存在则报错
        if(!f.exists()) {
            Panic.panic(Error.FileNotExistsException);
        }
        //文件无法读写则报错
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        return new TransactionManagerImpl(raf, fc);
    }
}
