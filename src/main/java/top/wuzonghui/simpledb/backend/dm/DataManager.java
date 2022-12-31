package top.wuzonghui.simpledb.backend.dm;

import top.wuzonghui.simpledb.backend.dm.dataitem.DataItem;
import top.wuzonghui.simpledb.backend.dm.logger.Logger;
import top.wuzonghui.simpledb.backend.dm.page.PageOne;
import top.wuzonghui.simpledb.backend.dm.pagecache.PageCache;
import top.wuzonghui.simpledb.backend.tm.TransactionManager;

/**
 * @author Starry
 * @create 2022-12-28-5:26 PM
 * @Describe 1.DataManager是DM层对外提供方法的类。同时也实现对DataItem对象的引用计数缓存。
 * 2.DataItem存储的key，是由页号和页内偏移组成的8字节无符号整数，页号和偏移各占4个字节。
 * 3.DM层对外提供了三个功能，分别是读、插入和修改，修改是通过读出DataItem然后修改实现的。
 */
public interface DataManager {
    /**
     * 根据uid，读取对应的DataItem对象。
     *
     * @param uid DataItem的uid，uid由2个部分组成：DataItem对象实际存储的Page的pageNumber和在page中的offset。
     * @return 读取到的DataItem对象。
     * @throws Exception
     */
    DataItem read(long uid) throws Exception;

    /**
     * 插入数据。
     * 将data插入到数据库中，实际上插入Page的是由data封装出的DataItem，[ValidFlag,1B][DataSize,2B][Data]。
     * 插入到哪一个Page，插入到Page的哪个offset，是由空闲位置决定的，可以通过返回值解析出这个位置。
     * @param xid  执行插入操作的事务的xid。
     * @param data 要插入到数据库中的数据。
     * @return 实际插入到Page中的DataItem的uid，通过该uid可以获取到DataItem。
     * @throws Exception
     */
    long insert(long xid, byte[] data) throws Exception;

    /**
     * 释放当前DataManager。
     * 1.会释放所有缓存的DataItem对象。
     * 2.会释放依赖的Logger对象。
     * 3.调用PageOne的setVcClose(Page pageOne)方法，将.db文件中第一页的100-107位，复制到108-115位。
     * 4.释放依赖的PageCache对象。
     */
    void close();

    /**
     * 创建一个DataManager对象并返回
     *
     * @param path   路径
     * @param memory 分配的内存大小
     * @param tm     TransactionManager对象
     * @return
     */
    static DataManager create(String path, long memory, TransactionManager tm) {
        //快速创建PageCache对象。
        PageCache pageCache = PageCache.create(path, memory);
        //快速创建Logger对象。
        Logger logger = Logger.create(path);
        //创建DataManagerImpl对象。
        DataManagerImpl dataManager = new DataManagerImpl(pageCache, logger, tm);

        dataManager.initPageOne();
        return dataManager;
    }

    /**
     * 以打开的方式，根据已有的文件创建DataManager对象，并返回。
     * 由于不是第一次打开文件，因此需要对第一页进行校验
     *
     * @param path   路径
     * @param memory 分配的内存空间大小
     * @param tm     TransactionManager对象
     * @return
     */
    static DataManager open(String path, long memory, TransactionManager tm) {
        //快速创建PageCache对象。
        PageCache pageCache = PageCache.open(path, memory);
        //快速创建Logger对象。
        Logger logger = Logger.open(path);

        DataManagerImpl dataManager = new DataManagerImpl(pageCache, logger, tm);
        //如果第一页的校验和不满足,则证明需要调用恢复例程
        if (!dataManager.loadCheckPageOne()) {
            Recover.recover(tm, logger, pageCache);
        }
        //初始化pageIndex
        dataManager.fillPageIndex();
        //重新设置第一页的随机字节序列
        PageOne.setVcOpen(dataManager.pageOne);
        //第一页刷盘
        dataManager.pageCache.flushPage(dataManager.pageOne);
        return dataManager;
    }
}
