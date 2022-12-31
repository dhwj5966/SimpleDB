package top.wuzonghui.simpledb.backend.dm.pageindex;

import top.wuzonghui.simpledb.backend.dm.pagecache.PageCache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Starry
 * @create 2022-12-28-1:17 AM
 * @Describe 页面索引。缓存了每一页的空闲空间，用于在上层模块插入数据的时候，快速找到一个合适空间的页面，而不是检查每一个Page。
 * 具体算法：将一个Page的空间划分成40个区间，在启动的时候遍历所有的页面信息，安排到这40个区间中
 * insert在请求一个页的时候，会首先将所需的空间向上取整，映射到某一个区间，随后取出这个区间的任何一页，即可满足需求。
 * @Detail 1.内存中以Page为单位管理数据，每个Page为8 * 1024 byte，每个Page可能已经存储了一些数据，一个Page的freespace代表该Page空闲空间大小。
 * 2.当上层模块要插入数据的时候，需要找到一个有合适空间的Page，但是如果要一个个从磁盘或缓存中检查Page，效率较低。
 * 3.因此，采用如下方法实现Page的空闲空间的索引。将每个Page从逻辑上划分为40个区域；
 *
 *
 */
public class PageIndex {
    /**
     * 划分出40个区间
     */
    private static final int INTERVALS_NO = 40;
    /**
     * 每个区间的大小
     */
    private static final int THRESHOLD = PageCache.PAGE_SIZE / INTERVALS_NO;

    private Lock lock;
    private List<PageInfo>[] lists;

    public PageIndex() {
        lock = new ReentrantLock();
        lists = new List[INTERVALS_NO+1];
        for (int i = 0; i < INTERVALS_NO+1; i++) {
            lists[i] = new ArrayList<>();
        }
    }

    /**
     * 在lists中寻找最小能满足存储需求的page对应的PageInfo对象
     * @param spaceSize 需求的空间大小
     * @return
     */
    public PageInfo select(int spaceSize) {
        lock.lock();
        try {
            int index = spaceSize / THRESHOLD;
            if (index < INTERVALS_NO) index++;
            while (true) {
                if (index > INTERVALS_NO) {
                    return null;
                }
                List<PageInfo> list = lists[index];
                if (list.size() == 0) {
                    index++;
                } else {
                    PageInfo pageInfo = list.get(0);
                    list.remove(0);
                    return pageInfo;
                }
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * @Describe 根据页的空闲空间大小和页号，将页的信息封装成PageInfo对象，
     * 并根据Page的freespace的大小，计算Page还有多少剩余的区间，并存到lists数组的对应位置
     * @param pgno 页号
     * @param freeSpace 该页剩余的空闲空间大小
     * @Instance 比如传入pgno = 4， freespace = 6000。
     * 则首先创建PageInfo对象，并根据 int index = freespace / 每个区间的大小，
     * 将PageInfo对象存到lists的index位所在的List中。
     */
    public void add(int pgno, int freeSpace) {
        lock.lock();
        try {
            //index代表这些剩余的freeSpace，能凑出多少个区间。
            int index = freeSpace / THRESHOLD;
            lists[index].add(new PageInfo(pgno, freeSpace));
        } finally {
            lock.unlock();
        }
    }


}
