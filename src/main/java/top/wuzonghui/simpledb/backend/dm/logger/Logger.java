package top.wuzonghui.simpledb.backend.dm.logger;

import top.wuzonghui.simpledb.backend.utils.Panic;
import top.wuzonghui.simpledb.backend.utils.Parser;
import top.wuzonghui.simpledb.common.Error;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author Starry
 * @create 2022-12-25-12:43 PM
 * @Describe SimpleDataBase提供了数据库崩溃后的数据恢复功能。DM模块每次对底层数据操作时，都会记录一条日志到磁盘上。
 * 在数据库崩溃后，再次启动的时候可以根据日志的内容，恢复数据文件，保证一致性。
 * 该接口定义了一个Logger应有的方法，每个Logger应该对应一个.log文件。
 */
public interface Logger {

    /**
     * 根据data中的数据，生成一条新的日志，并追加到日志文件的末尾。
     * 该操作会更新整个日志文件的校验和。
     * @param data
     */
    void log(byte[] data);

    /**
     * 将日志文件的长度截断为x字节
     * @param x
     * @throws Exception
     */
    void truncate(long x) throws Exception;

    /**
     * 迭代器设计模式，获取下一条日志的data
     * @return
     */
    byte[] next();

    /**
     * 配合next使用，将指针挪回第一条log的位置，再次调用next()方法将获取到第一条日志的data
     */
    void rewind();

    /**
     * 释放资源
     */
    void close();

    /**
     * 根据指定路径，创建.log结尾的日志文件，创建并返回对应的Logger对象
     * @param path
     * @return
     */
    public static Logger create(String path) {
        //路径与path拼接，创建出file对象
        File file = new File(path + LoggerImpl.LOG_SUFFIX);
        //需要文件可以创建，并且可以读写
        try {
            if (!file.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        } catch (IOException e) {
            Panic.panic(e);
        }
        if (!file.canWrite() || !file.canRead()) {
            Panic.panic(Error.FileCannotRWException);
        }

        //创建文件的RandomAccessFile和FileChannel对象。
        RandomAccessFile randomAccessFile = null;
        FileChannel fileChannel = null;
        try {
            randomAccessFile = new RandomAccessFile(file, "rw");
            fileChannel = randomAccessFile.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        //将新建的文件的前4位写为0
        try {
            fileChannel.position(0);
            fileChannel.write(ByteBuffer.wrap(Parser.int2Byte(0)));
            fileChannel.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
        return new LoggerImpl(randomAccessFile, fileChannel, 0);
    }

    /**
     * 根据指定路径，根据以.log文件结尾的日志文件，对.log文件进行检查，如果合法则创建对应Logger对象并返回。
     * @param path
     * @return
     */
    public static Logger open(String path) {
        File f = new File(path+LoggerImpl.LOG_SUFFIX);
        if(!f.exists()) {
            Panic.panic(Error.FileNotExistsException);
        }
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

        LoggerImpl lg = new LoggerImpl(raf, fc);

        //对文件进行检查
        lg.init();

        return lg;
    }
}
