package top.wuzonghui.simpledb.backend.dm;

import top.wuzonghui.simpledb.backend.common.AbstractCache;
import top.wuzonghui.simpledb.backend.dm.dataitem.DataItem;
import top.wuzonghui.simpledb.backend.dm.dataitem.DataItemImpl;
import top.wuzonghui.simpledb.backend.dm.logger.Logger;
import top.wuzonghui.simpledb.backend.dm.page.Page;
import top.wuzonghui.simpledb.backend.dm.page.PageOne;
import top.wuzonghui.simpledb.backend.dm.page.PageX;
import top.wuzonghui.simpledb.backend.dm.pagecache.PageCache;
import top.wuzonghui.simpledb.backend.dm.pageindex.PageIndex;
import top.wuzonghui.simpledb.backend.dm.pageindex.PageInfo;
import top.wuzonghui.simpledb.backend.tm.TransactionManager;
import top.wuzonghui.simpledb.backend.utils.Panic;
import top.wuzonghui.simpledb.common.Error;


/**
 * @author Starry
 * @create 2022-12-28-5:26 PM
 * @Describe DataManager的默认实现类
 */
public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager{
    /*
        分析：
        1.DataManager是DM层对外提供方法的类，上层不直接操作Page，而是操作DataItem。
        2.DataManager需要对DataItem也实现引用计数缓存策略，因此需要继承AbstractCache类，并实现两个钩子方法。
        3.DataManager对数据的操作依赖于Page对象，因此该类依赖PageCache。
        4.DataManager需要对日志进行修改，因此需要依赖Logger。
        5.其他依赖的类有TransactionManager、PageIndex、Page。
     */

    /**
     * 用来操作Page
     */
    PageCache pageCache;
    Logger logger;
    TransactionManager transactionManager;

    /**
     * 当前数据库的pageIndex对象，对应一个.db文件
     */
    PageIndex pageIndex;
    Page pageOne;

    public DataManagerImpl(PageCache pageCache, Logger logger, TransactionManager transactionManager) {
        super(0);
        this.pageCache = pageCache;
        this.logger = logger;
        this.transactionManager = transactionManager;
        pageIndex = new PageIndex();
    }

    @Override
    public DataItem read(long uid) throws Exception {
        //调用get方法，通过缓存读。
        DataItemImpl dataItem = (DataItemImpl) super.get(uid);
        if (dataItem.isValid()) {
            dataItem.release();
            return null;
        }
        return dataItem;
    }

    @Override
    public long insert(long xid, byte[] data) throws Exception {
        //根据要插入到数据库中的数据，封装出实际要插入的byte数组，即在数据前加入三个字节，第一个字节是Valid，第二个和第三个字节标识数据长度。
        //[ValidFlag,1byte],[DataSize,2byte],[Data]
        byte[] raw = DataItem.wrapDataItemRaw(data);
        //如果要插入的长度甚至大于了一个Page的最大长度(即8K - 2)，报错
        if(raw.length > PageX.MAX_FREE_SPACE) {
            throw Error.DataTooLargeException;
        }

        PageInfo pi = null;
        //循环5次去获取pi,如果获取到就立刻退出循环，如果不存在能提供合适空间的page，那么就新增一个page。
        for(int i = 0; i < 5; i ++) {
            pi = pageIndex.select(raw.length);
            if (pi != null) {
                break;
            } else {
                int newPgno = pageCache.newPage(PageX.initRaw());
                pageIndex.add(newPgno, PageX.MAX_FREE_SPACE);
            }
        }
        if(pi == null) {
            throw Error.DatabaseBusyException;
        }


        Page pg = null;
        int freeSpace = 0;
        try {
            //根据pageinfo，获取page
            pg = pageCache.getPage(pi.pgno);
            //生成insertLog，并追加到.log文件中
            byte[] log = Recover.insertLog(xid, pg, raw);
            logger.log(log);

            //在日志落盘后再将数据追加到page中
            short offset = PageX.insert(pg, raw);

            //释放page
            pg.release();

            return DataItem.addressToUid(pi.pgno, offset);

        } finally {
            // 将取出的pg重新插入pIndex
            if(pg != null) {
                pageIndex.add(pi.pgno, PageX.getFreeSpace(pg));
            } else {
                pageIndex.add(pi.pgno, freeSpace);
            }
        }
    }

    @Override
    public void close() {
        super.close();
        logger.close();

        PageOne.setVcClose(pageOne);
        pageOne.release();
        pageCache.close();
    }

    //key指明了封装DataItem的全部信息，即pageno和offset
    @Override
    protected DataItem getForCache(long key) throws Exception {
        //key 前四位是pgno，后四位是offset
        short offset = (short) (key & ((1 << 16) - 1));
        key >>>= 32;
        int pgno = (int) (key & ((1 << 32) - 1));
        Page page = pageCache.getPage(pgno);
        return DataItem.parseDataItem(page, offset, this);
    }

    @Override
    protected void releaseForCache(DataItem dataItem) {
        //释放dataItem所在页即可
        dataItem.page().release();
    }

    /**
     * 生成一条updateLog，并追加到.log文件的末尾。
     * @Detail
     * 1.调用Recover的工具方法，生成一条updateLog。
     * 2.调用logger对象的log(byte[] data)方法，将日志追加到.log文件的末尾。
     * @param xid
     * @param di
     */
    public void logDataItem(long xid, DataItem di) {
        //调用Recover的工具方法，生成一条updateLog。
        byte[] log = Recover.updateLog(xid, di);
        //将log追加到.log文件的末尾
        logger.log(log);
    }

    /**
     * 释放DataItem。
     * @param di
     */
    public void releaseDataItem(DataItem di) {
        super.release(di.getUid());
    }


    /**
     * 创建文件时需要对第一页特殊初始化，由于第一页是用来校验的，所以需要特殊初始化。
     */
    void initPageOne() {
        //新建第一页
        byte[] pageOneBytes = PageOne.InitRaw();
        int pageOneNo = pageCache.newPage(pageOneBytes);
        //将第一页加载到缓存中，并且注入pageOne字段。
        try {
            this.pageOne = pageCache.getPage(pageOneNo);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        //直接将第一页刷入到文件系统中。
        pageCache.flushPage(pageOne);
    }

    /**
     * 在以打开的方式创建DataManager对象的时候，需要对第一页的正确性进行检验。
     * @return true:合法。false:非法
     */
    boolean loadCheckPageOne() {
        try {
            this.pageOne = pageCache.getPage(1);
        } catch (Exception e) {
            Panic.panic(e);
        }
        return PageOne.checkVc(pageOne);
    }

    /**
     * 初始化pageIndex，只有打开的方式创建DataManager对象的时候调用，因为如果是创建的方式，不存在已有Page。
     */
    void fillPageIndex() {
        int numberOfPage = pageCache.getPageNumber();
        for (int i = 2; i < numberOfPage; i++) {
            Page page = null;
            try {
                page = pageCache.getPage(i);
                int freeSpace = PageX.getFreeSpace(page);
                pageIndex.add(page.getPageNumber(), freeSpace);
            } catch (Exception e) {
                Panic.panic(e);
            } finally {
                page.release();
            }
        }
    }
}
