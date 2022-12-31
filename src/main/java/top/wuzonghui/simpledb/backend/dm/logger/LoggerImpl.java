package top.wuzonghui.simpledb.backend.dm.logger;

import com.google.common.primitives.Bytes;
import top.wuzonghui.simpledb.backend.utils.Panic;
import top.wuzonghui.simpledb.backend.utils.Parser;
import top.wuzonghui.simpledb.common.Error;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Starry
 * @create 2022-12-25-12:43 PM
 * @Describe
 * 日志文件读写
 *
 *  日志文件标准格式为：
 *  [XChecksum] [Log1] [Log2] ... [LogN] [BadTail]
 *  XChecksum 为后续所有日志计算的Checksum，int类型
 *
 *  每条正确日志的格式为：
 *  [Size] [Checksum] [Data]
 *  Size 4字节int 标识Data长度
 *  Checksum 4字节int
 */
public class LoggerImpl implements Logger{
    //协助计算校验和的一个数字
    private static final int SEED = 13331;
    /**
     * 每条log的Size区的起始位置
     */
    private static final int OF_SIZE = 0;
    /**
     * 每条log的CheckSum区的起始位置
     */
    private static final int OF_CHECKSUM = OF_SIZE + 4;
    /**
     * 每条log的数据区的起始位置
     */
    private static final int OF_DATA = OF_CHECKSUM + 4;

    public static final String LOG_SUFFIX = ".log";

    private RandomAccessFile file;

    private FileChannel fileChannel;

    private Lock lock;

    /**
     * fileChannel当前的指针
     */
    private long position;

    /**
     * 文件的长度，初始化的时候记录，log操作的时候不更新
     */
    private long fileSize;

    /**
     * 整个日志文件的校验和
     */
    private int xCheckSum;

    public LoggerImpl(RandomAccessFile file, FileChannel fileChannel) {
        this.file = file;
        this.fileChannel = fileChannel;
        lock = new ReentrantLock();
    }

    public LoggerImpl(RandomAccessFile file, FileChannel fileChannel, int xCheckSum) {
        this.file = file;
        this.fileChannel = fileChannel;
        this.xCheckSum = xCheckSum;
        lock = new ReentrantLock();
    }

    @Override
    public void log(byte[] data) {
        //首先需要将一条log包装出来，一条log包括size区，checksum区，data区
        byte[] log = wrapLog(data);
        //将byte数组放到缓冲区里
        ByteBuffer buffer = ByteBuffer.wrap(log);
        //对于文件的读写操作要上锁
        lock.lock();
        try {
            //由于新的log要追加到文件末尾，因此将fileChannel的指针移动到fileChannel最后
            fileChannel.position(fileChannel.size());
            fileChannel.write(buffer);
        } catch (Exception e) {
            Panic.panic(e);
        } finally {
            lock.unlock();
        }
        //由于追加了新的log，因此要更新整个log文件的校验和
        updateXChecksum(log);
    }

    /**
     * 在日志文件的末尾追加了byte[] log数组后，需要将整个日志文件的校验和更新
     * @param log
     */
    private void updateXChecksum(byte[] log) {
        //先把成员变量更新一下
        this.xCheckSum = calCheckSum(xCheckSum, log);
        //将值刷入磁盘
        try {
            fileChannel.position(0);
            fileChannel.write(ByteBuffer.wrap(Parser.int2Byte(xCheckSum)));
            fileChannel.force(false);
        } catch (Exception e) {
            Panic.panic(e);
        }
    }


    @Override
    public void truncate(long x) throws Exception {
        lock.lock();
        try {
            fileChannel.truncate(x);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public byte[] next() {
        //由于该操作需要从文件系统中读取数据，因此需要上锁。
        lock.lock();
        try {
            //获取整个log数据
            byte[] log = internNext();
            if (log == null) return null;
            return Arrays.copyOfRange(log, OF_DATA, log.length);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 解析下一条日志的完整信息，并返回。
     * 必须是一条完整的日志，即满足data区长度为size，data区数据计算出的校验和和checksum区数据一致。
     * @return 包括日志的size区、check区和data区。如果已至文件末尾或日志出错，则返回null。
     */
    private byte[] internNext() {
        //1.先获取log的size
        if (position + OF_DATA > fileSize) {
            return null;
        }

        ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
        try {
            fileChannel.position(position);
            fileChannel.read(sizeBuffer);
        } catch (IOException e) {
            Panic.panic(e);
        }

        int size = Parser.parseInt(sizeBuffer.array());

        if (position + OF_DATA + size > fileSize) {
            return null;
        }

        //2.再获取整条log
        byte[] log = null;
        try {
            ByteBuffer logBuffer = ByteBuffer.allocate(8 + size);
            fileChannel.position(position);
            fileChannel.read(logBuffer);
            log = logBuffer.array();
        } catch (IOException e) {
            Panic.panic(e);
        }

        //3.检验校验和是否一致
        int check1 = Parser.parseInt(Arrays.copyOfRange(log, OF_CHECKSUM, OF_DATA));
        int check2 = calCheckSum(0, Arrays.copyOfRange(log, OF_DATA, OF_DATA + size));
        if (check1 != check2) {
            return null;
        }
        position += log.length;
        return log;
    }


    @Override
    public void rewind() {
        position = 4;
    }

    @Override
    public void close() {
        try {
            fileChannel.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    //当xCheck = 0,计算一条log的校验和
    private int calCheckSum(int xCheck, byte[] data) {
        for (byte b : data) {
            xCheck = xCheck * SEED + b;
        }
        return xCheck;
    }

    /**
     * 根据一条log的数据区，生成一整条log的byte数组。给数据库前面加上size区和checksum区即可
     * @param data 一条log的data区
     * @return 一条log的完整数据
     */
    private byte[] wrapLog(byte[] data) {
        byte[] size = Parser.int2Byte(data.length);
        byte[] checkSum = Parser.int2Byte(calCheckSum(0, data));
        //利用com.google.common.primitives.Bytes工具类，将3个byte数组合成。
        return Bytes.concat(size, checkSum, data);
    }

    /**
     * 打开一个.log文件时，需要校验文件的xCheckSum。并移除文件尾部可能存在的BadTail。
     * BadTail代表该条日志尚未完整写入，因此文件的校验和不会包含BadTail的校验和。
     */
    public void init() {
        //1.获取整个文件的长度
        long size = 0;
        try {
            size = file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        if(size < 4) {
            Panic.panic(Error.BadLogFileException);
        }

        //读取.log文件开头的4个字节,即整个文件的校验和
        ByteBuffer raw = ByteBuffer.allocate(4);
        try {
            fileChannel.position(0);
            fileChannel.read(raw);
        } catch (IOException e) {
            Panic.panic(e);
        }
        int xChecksum = Parser.parseInt(raw.array());
        this.fileSize = size;
        this.xCheckSum = xChecksum;

        checkAndRemoveTail();
    }

    //检查并移除BadTail
    private void checkAndRemoveTail() {
        rewind();

        int xCheck = 0;
        //一个个log读取，并累加计算校验和，但是如果最后一个日志坏掉了则不计算校验和
        while(true) {
            byte[] log = internNext();
            if(log == null) break;
            xCheck = calCheckSum(xCheck, log);
        }
        if(xCheck != xCheckSum) {
            Panic.panic(Error.BadLogFileException);
        }

        try {
            //截取文件
            truncate(position);
        } catch (Exception e) {
            Panic.panic(e);
        }
        try {
            file.seek(position);
        } catch (IOException e) {
            Panic.panic(e);
        }
        rewind();
    }
}
