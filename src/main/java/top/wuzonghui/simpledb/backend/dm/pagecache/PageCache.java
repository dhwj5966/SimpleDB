package top.wuzonghui.simpledb.backend.dm.pagecache;

import top.wuzonghui.simpledb.backend.dm.page.Page;
import top.wuzonghui.simpledb.backend.utils.Panic;
import top.wuzonghui.simpledb.common.Error;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

/**
 * @author Starry
 * @create 2022-12-24-2:24 PM
 * @Describe 页的缓存类，在文件系统和内存间搭建一层缓存，每个PageCache对象应该对应一个.db后缀的数据库文件。
 * 该类的作用主要是沟通文件系统以及内存，并在之间实现缓存的效果，而不是每次都要去文件系统中读数据。
 */
public interface PageCache {
    int PAGE_SIZE = 1 << 13;

    /**
     * 根据传入的数据，创建一个新的数据页，并且立刻写入到文件中(写入完毕后，内存中不再有该对象)
     * @param initData 用来创建新数据页的数据
     * @return 新写入文件的数据页的页号
     */
    int newPage(byte[] initData);

    /**
     * 获取该文件中指定页号的Page对象
     * @param pgno 页号
     * @return 指定页号的Page对象
     * @throws Exception
     */
    Page getPage(int pgno) throws Exception;

    /**
     * 关闭当前缓存，释放资源
     */
    void close();

    /**
     * 释放数据页，将当前数据页从内存中释放。注意：具体能不能释放要判断引用计数。
     * @param page 指定数据页
     */
    void release(Page page);

    /**
     *  根据传入的maxPgno截断.db文件。比如一个数据页的大小是8K，当前有11页，那么文件大小就是88K。
     *  如果调用该方法并传入8，则只能保留8个数据页，文件大小被截断为64K。
     * @param maxPgno
     */
    void truncateByBgno(int maxPgno);

    /**
     * 获取当前缓存对应的数据库文件拥有的总页数
     * @return
     */
    int getPageNumber();

    /**
     * 将当前数据页刷入文件系统中。
     * @param pg
     */
    void flushPage(Page pg);

    /**
     * 按指定路径，创建.db文件并创建该文件对应的PageCache对象。
     * @param path
     * @param memory 创建的PageCache占据的最大内存数
     * @return
     */
    public static PageCache create(String path, long memory) {
        //需要有多种合法性检验。1。是否可以创建文件 2.是否可以读写文件
        File file = new File(path + PageCacheImpl.DB_SUFFIX);
        try {
            if (!file.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        } catch (IOException e) {
            Panic.panic(e);
        }
        if (!file.canRead() || !file.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }
        RandomAccessFile randomAccessFile = null;
        FileChannel fileChannel = null;
        try {
            randomAccessFile = new RandomAccessFile(file, "rw");
            fileChannel = randomAccessFile.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        return new PageCacheImpl(randomAccessFile, fileChannel, (int) memory / PageCache.PAGE_SIZE);
    }

    /**
     * 按指定路径，通过已存在的.db文件创建对应的PageCache对象
     * @param path
     * @param memory
     * @return
     */
    public static PageCache open(String path, long memory) {
        //合法性检验有所不同。 1.文件是否存在 2.文件是否可以读写
        File file = new File(path + PageCacheImpl.DB_SUFFIX);
        if (!file.exists()) {
            Panic.panic(Error.FileNotExistsException);
        }
        if (!file.canRead() || !file.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }
        RandomAccessFile randomAccessFile = null;
        FileChannel fileChannel = null;
        try {
            randomAccessFile = new RandomAccessFile(file, "rw");
            fileChannel = randomAccessFile.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        return new PageCacheImpl(randomAccessFile, fileChannel, (int) memory / PageCache.PAGE_SIZE);
    }
}
