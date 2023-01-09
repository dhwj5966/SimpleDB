package top.wuzonghui.simpledb.backend.common;

import top.wuzonghui.simpledb.common.Error;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Starry
 * @create 2022-12-23-7:57 PM
 * @Describe 实现了引用计数策略的缓存。引用计数策略的具体含义是：记录每个缓存数据被上层模块引用的次数，
 * 一个数据只有完全不被上层模块引用了，才能从缓存中驱逐，有点像JVM。
 * 采用这种策略而不是其他策略(比如著名的LRU)是因为(这里还没完全看懂为什么不用LRU，解决了回头补充)
 * ·子类可以通过实现该抽象类，并实现getForCache(long key)和releaseForCache(T obj)
 * 方法快速实现引用计数缓存。
 */
public abstract class AbstractCache<T> {
    //缓存数据就存放在该HashMap中
    private HashMap<Long, T> cache;
    //记录指定key的对象，有多少个引用
    private HashMap<Long, Integer> references;
    /*
        正在被获取的资源。调用该类的get方法，会首先尝试从缓存中获取数据。
        如果缓存中没有该数据，就会通过某种方式去获取，一般是从硬盘里读，这个过程相当耗时，所以如果已经着手从硬盘里去读了，
        那么后续对这个数据的获取就不要再从硬盘里去读了，而应该等读的结果，然后从缓存里获取。
     */
    private HashMap<Long, Boolean> getting;
    // 缓存的最大缓存资源数，设为0则说明不限制。
    private int maxResource;
    // 缓存中元素的个数
    private int count = 0;
    private Lock lock;

    //构造方法，需要传入最大缓存的资源数量
    public AbstractCache(int maxResource) {
        this.maxResource = maxResource;
        cache = new HashMap<>();
        references = new HashMap<>();
        getting = new HashMap<>();
        lock = new ReentrantLock();
    }

    /**
     * 从缓存中获取数据。策略如下：
     * 1.首先检查getting中是否存在资源，如果getting中存在资源则说明系统正在从硬盘中将该资源读取到缓存中，那么则需要短暂的等待,让当前线程sleep 1ms并再次查看。
     * 2.如果资源在缓存中，则直接返回当前资源，并引用计数+1
     * 3.如果既不在getting中，也不在缓存中，则需要调用getForCache()方法获取该资源，在获取之前需要检查缓存能容纳的最大资源数是否已满，满则报错。
     * 4.调用getForCache()方法前需要将key放入到getting中，在获取后再删除getting中数据。(以便其他线程不用再针对该资源调用getForCache())
     * 5.注意！一切对几个map的操作都要上锁
     * @param key 通过key指定要获取的资源
     * @return 要获取的资源
     * @throws Exception
     */
    protected T get(long key) throws Exception{
        //循环尝试
        while (true) {
            lock.lock();
            //key在getting中则说明要等待,那么就解锁，并sleep 1ms
            if (getting.containsKey(key)) {
                lock.unlock();
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    continue;
                }
                continue;
            }
            //如果key不在getting中，那么就尝试从缓存中获取
            if (cache.containsKey(key)) {
                lock.unlock();
                T value = cache.get(key);
                //获取数据不要忘了更新该资源的引用计数
                references.put(key, references.get(key) + 1);
                return value;
            }
            //如果key既不在getting中，也不在缓存中，那么就需要去硬盘中get,去硬盘中取放到循环外
            if (maxResource > 0 && count == maxResource) {
                lock.unlock();
                throw Error.CacheFullException;
            }
            break;
        }

        count++;
        getting.put(key, true);
        lock.unlock();

        T obj = null;
        try {
            obj = getForCache(key);
        } catch (Exception e) {
            //如果发生异常,需要将相应数据恢复，对数据的恢复操作也要上锁
            lock.lock();
            getting.remove(key);
            count--;
            lock.unlock();
            throw e;
        }
        //如果从getForCache()方法中正确获取到了数据
        lock.lock();
        //该资源不再处于正在获取的状态
        getting.remove(key);
        //该资源引用计数为1（刚从硬盘中取出来的热乎数据，引用计数肯定是1）
        references.put(key, 1);
        //将该资源放入缓存中
        cache.put(key, obj);
        lock.unlock();
        return obj;
    }

    /**
     * 释放一个资源，注意，调用该方法的线程只能将自己对该资源的占用释放，如果还有其他线程引用该资源，那么该资源还是会存在在缓存中
     * @param key
     */
    protected void release(long key) {
        lock.lock();
        try {
            int ref = references.get(key) - 1;
            //如果引用计数已经为0了,彻底释放该资源
            if (ref == 0) {
                T obj = cache.get(key);
                releaseForCache(obj);
                count--;
                cache.remove(key);
                references.remove(key);
            } else {
                references.put(key, ref);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 关闭缓存，写回所有资源,遍历Cache，将Cache中每个资源调用releaseForCache(obj)方法写回硬盘
     */
    protected void close() {
        lock.lock();
        try {
            Set<Long> keys = cache.keySet();
            //遍历缓存中所有的key，对于每个key
            for (Long key : keys) {
                T obj = cache.get(key);
                releaseForCache(obj);
                references.remove(key);
                cache.remove(key);
            }
        } finally {
            count = 0;
            lock.unlock();
        }
    }

    /**
     * 当资源不在缓存时,获取行为该资源，具体获取策略由子类实现
     * @param key
     * @return 获取到的该资源
     */
    protected abstract T getForCache(long key) throws Exception;

    /**
     * 当资源被驱逐时的写回行为
     */
    protected abstract void releaseForCache(T obj);
}
