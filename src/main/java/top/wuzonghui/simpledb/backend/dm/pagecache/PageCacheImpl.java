package top.wuzonghui.simpledb.backend.dm.pagecache;

import top.wuzonghui.simpledb.backend.common.AbstractCache;
import top.wuzonghui.simpledb.backend.dm.page.Page;
import top.wuzonghui.simpledb.backend.dm.page.PageImpl;
import top.wuzonghui.simpledb.backend.utils.Panic;
import top.wuzonghui.simpledb.common.Error;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Starry
 * @create 2022-12-24-2:36 PM
 * @Describe PageCache的默认实现类,沟通内存与文件系统。
 */
public class PageCacheImpl extends AbstractCache<Page> implements PageCache{

    private static final int MEM_MIN_LIM = 10;
    /**
     * 数据存放的文件的后缀名
     */
    public static final String DB_SUFFIX = ".db";

    /**
     * 可以操作数据库文件的RandomAccessFile对象
     */
    private RandomAccessFile file;

    /**
     * file对象的Channel
     */
    private FileChannel fc;

    /**
     * 文件锁，确保对文件的读写是单线程的，以免造成数据不一致
     */
    private Lock fileLock;

    /**
     * 记录总页数
     */
    private AtomicInteger pageNumbers;

    //需要将操作的数据库文件对应的RandomAccessFile对象和FileChannel对象传入。
    public PageCacheImpl(RandomAccessFile file, FileChannel fileChannel, int maxResource) {
        super(maxResource);
        //如果maxResource小于规定值，则认为太小。
        if(maxResource < MEM_MIN_LIM) {
            Panic.panic(Error.MemTooSmallException);
        }
        long length = 0;
        try {
            length = file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        //一些初始化
        this.file = file;
        this.fc = fileChannel;
        this.fileLock = new ReentrantLock();
        //读取到当前数据库文件的Page的数量
        this.pageNumbers = new AtomicInteger((int)length / PAGE_SIZE);
    }

    //当页不在当前缓存中，应去文件系统中读取数据，并封装成Page返回
    @Override
    protected Page getForCache(long key) throws Exception {
        //要读取的key就是页号
        int pgno = (int) key;
        //获取要读取的页号的数据，在文件中的偏移量
        long offset = PageCacheImpl.pageOffset(pgno);
        //分配一个页大小的缓冲空间,用来存放页数据
        ByteBuffer buffer = ByteBuffer.allocate(PAGE_SIZE);

        //对文件的操作需要加锁，不可以多个线程同时读。
        fileLock.lock();
        try {
            //调转指针，读取数据到缓冲区
            fc.position(offset);
            fc.read(buffer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        fileLock.unlock();
        //将刚读取到的数据，封装成一个数据页Page，并返回
        return new PageImpl(pgno, buffer.array(), this);
    }

    /**
     * 根据页号，返回页数据在文件中的偏移量，页号从1开始。
     * 比如一个PAGE_SIZE为8K，则pgno为1的页的存放位置就0，pgno为2的数据存放的位置就是8K。
     * @param pgno
     * @return
     */
    public static long pageOffset(int pgno) {
        return (pgno - 1) * PAGE_SIZE;
    }

    //将页写回到硬盘中,如果该页是脏页再进行写出，并且将改页置为非脏页
    @Override
    protected void releaseForCache(Page obj) {
        if (obj.isDirty()) {
            flushPage(obj);
            obj.setDirty(false);
        }
    }

    @Override
    public int newPage(byte[] initData) {
        //
        int pgno = pageNumbers.incrementAndGet();
        //根据新传入的数据，新建一个Page对象并且立刻写入到文件中
        Page pg = new PageImpl(pgno, initData, null);
        //刷入文件
        flushPage(pg);
        return pgno;
    }

    @Override
    public Page getPage(int pgno) throws Exception {
        return get((long)pgno);
    }

    @Override
    public void close() {
        super.close();
        try {
            file.close();
            fc.close();
        } catch (Exception e) {
            Panic.panic(e);
        }
    }

    @Override
    public void release(Page page) {
        release((long) page.getPageNumber());
    }

    @Override
    public void truncateByBgno(int maxPgno) {
        long size = PageCacheImpl.pageOffset(maxPgno + 1);
        try {
            file.setLength(size);
        } catch (Exception e){
            Panic.panic(e);
        }
        pageNumbers.set(maxPgno);
    }

    @Override
    public int getPageNumber() {
        return pageNumbers.intValue();
    }

    /**
     * 将paged的数据写入文件中
     * @param pg
     */
    @Override
    public void flushPage(Page pg) {
        int pgno = pg.getPageNumber();
        long offset = pageOffset(pgno);

        //由于要向db文件中写入页数据，因此需要加锁
        fileLock.lock();
        try {
            //将page的数据放到缓冲区内
            ByteBuffer buffer = ByteBuffer.wrap(pg.getData());
            //改变偏移量
            fc.position(offset);
            //写入到文件里
            fc.write(buffer);
            fc.force(false);
        } catch (Exception e) {
            Panic.panic(e);
        } finally {
            fileLock.unlock();
        }
    }
}
