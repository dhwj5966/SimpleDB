package top.wuzonghui.simpledb.backend.vm;

import top.wuzonghui.simpledb.common.Error;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Starry
 * @create 2023-01-02-2:01 PM
 * @Describe 该类在内存中维护一个依赖等待图，以进行死锁检测。
 */
public class LockTable {

    /**
     * @Describe 记录某个xid已经获得的资源的uid列表。
     * @Instance {xid1 = [uid1, uid3, uid17]}代表xid为xid1的事务，
     * 获取了uid为uid1，uid3，uid17的资源。
     */
    private Map<Long, List<Long>> x2u;  // 某个XID已经获得的资源的UID列表

    /**
     * @Describe 记录了uid被xid持有。
     */
    private Map<Long, Long> u2x;        // UID被某个XID持有

    /**
     * @Describe 记录了正在等待uid的xid列表。
     * @Instance {uid1 = [xid1, xid2, xid7]}代表uid为uid1的资源，
     * 有xid为xid1，xid2，xid7的三个事务正在等待获取。
     */
    private Map<Long, List<Long>> wait; // 正在等待UID的XID列表

    /**
     * 正在等待资源的XID,对应的锁对象。
     */
    private Map<Long, Lock> waitLock;

    /**
     * @Describe XID正在等待的UID。
     * @Instance {xid1 = uid7}代表xid1的事务在等待uid7的资源。
     */
    private Map<Long, Long> waitU;

    private Lock lock;

    public LockTable() {
        x2u = new HashMap<>();
        u2x = new HashMap<>();
        wait = new HashMap<>();
        waitLock = new HashMap<>();
        waitU = new HashMap<>();
        lock = new ReentrantLock();
    }

    /**
     * 当一个事务commit或aborted，就可以释放它持有的资源。
     * 当一个事务释放资源时，等待这些资源的xid会获取到该资源，并且解锁。
     * @param xid 要释放资源的事务的xid。
     */
    public void remove(long xid) {
        //1.由于可能有多个事务同时调用remove方法，因此需要加锁。
        lock.lock();
        try {
            /*
                1.当一个事务释放的时候，xid会释放所有占用的uid，因此需要利用wait字段，让一个xid可以占用该xid释放出的uid。
                2.清除各field中的记录
             */
            List<Long> list = x2u.get(xid);
            if (list != null) {
                while (!list.isEmpty()) {
                    Long uid = list.remove(0);
                    selectNewXID(uid);
                }
            }
            waitU.remove(xid);
            x2u.remove(xid);
            waitLock.remove(xid);

        } finally {
            lock.unlock();
        }
    }

    /**
     * xid代表的事务尝试获取uid代表的资源。
     * @param xid 尝试获取uid的xid。
     * @param uid xid试图获取的uid。
     * @return 不需要等待，直接获取资源则返回null。否则返回锁对象。
     * @throws Exception 会造成死锁则抛出异常。
     */
    public Lock add(long xid, long uid) throws Exception {
        //1.上锁。要上锁因为可能有多个事务同时尝试获取资源，可能导致数据不一致。
        lock.lock();

        try {
            //2.如果xid已经获得了uid，则说明不需要等待，返回null
            if (isInList(x2u, xid, uid)) {
                return null;
            }
            //3.如果该uid没有被任何xid占用，则也不需要等待，返回null。但是需要更新u2x和x2u。
            if (!u2x.containsKey(uid)) {
                u2x.put(uid, xid);
                //更新xid持有的uid。
                putIntoList(x2u, xid, uid);
                return null;
            }
            /*
                4.如果2和3都没有返回null，则说明xid既不是已经获得了uid，uid也不处于空闲状态，不能返回null，xid需要等待资源，需要返回锁对象。
                更新waitU。
                更新wait。
                如果发生了死锁，那么更新waitU，更新wait，并且报错。
                如果没发生死锁，那么新建一个重入锁，锁上，更新waitLock，返回。
             */
            waitU.put(xid, uid);
            putIntoList(wait, uid, xid);
            if (hasDeadLock()) {
                //如果xid对uid的获取导致了死锁，采取如下策略：撤销这条边，并撤销事务。
                waitU.remove(xid);
                removeFromList(wait, uid, xid);
                throw Error.DeadlockException;
            }
            Lock lock1 = new ReentrantLock();
            lock1.lock();
            waitLock.put(xid, lock1);
            return lock1;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 在等待uid的xid中，选择一个获取到该uid,会释放获取到该uid资源的xid的锁。
     */
    private void selectNewXID(long uid) {
        u2x.remove(uid);
        List<Long> list = wait.get(uid);
        if (list != null) {
            while (list.size() > 0) {
                long xid = list.remove(0);
                //
                if (!waitLock.containsKey(xid)) {
                    continue;
                } else {
                    u2x.put(uid, xid);
                    Lock lo = waitLock.remove(xid);
                    waitU.remove(xid);
                    lo.unlock();
                    break;
                }
            }
        }

        if (list.isEmpty()) wait.remove(uid);
    }

    /**
     * 从key为id0对应的value中，移除id1。
     */
    private void removeFromList(Map<Long, List<Long>> listMap, long id0, long id1) {
        List<Long> list = listMap.get(id0);
        if (list == null) {
            return;
        }
        list.remove(id1);
        if (list.size() == 0) {
            listMap.remove(id0);
        }
    }



    /**
     * @Describe 判断当前的图结构中是否存在死锁。
     * @return true:存在死锁。false:不存在死锁。
     * @Detail 通过带访问戳的dfs实现。
     * 图的节点：可以将一个节点视为一个xid；
     * 图的边：当xid0的节点等待获取uid0的时候，xid0节点就指向持有uid0的节点xid1。
     * 为什么需要访问戳，减少重复dfs。
     * 总体思路：遍历所有持有资源的节点，检查图是否有环，有环则说明存在死锁。
     * 如果node1指向node2，node2指向node3，那么node1，node2，node3的访问戳就相同，可以理解为一条线路，访问戳用Map记录。
     * 当node1经历过dfs后，node2和node3已经被记录到了访问戳里，所以再遍历到node2，node3的时候就可以直接知道node2和node3不存在环。
     * 这个访问戳就相当于一个memory，对dfs进行剪枝，在复杂图结构中会极大得提升遍历速度。
     * 当遍历到一个没有访问戳的节点，就开启一个新的访问戳，不断迭代遍历即可，并把整条线路路过的节点，都以{xid -> stamp}的形式记录在Map中。
     * 如果一个新的节点指向了已有的线路，那么说明该节点不存在环，但是如果一个新节点的线路延申到了自己所在线路上，则说明有环。
     * 此外，如果线路不再延申，也就是说当前node不再有出去的边，也就是当前xid没有正在等待的uid，那么线路断掉，不存在环。
     * @Instance xid3持有uid4，等待uid7。xid6持有uid7，等待uid4。发生了循环等待，即死锁。
     */
    private boolean hasDeadLock() {
        xidStamp = new HashMap<>();
        stamp = 1;
        //遍历所有持有资源的xid。
        for (long xid : x2u.keySet()) {

            Integer sta = xidStamp.get(xid);
            //如果该xid已经属于某个结构。
            if(sta != null && sta > 0) {
                continue;
            }

            stamp++;
            if (dfs(xid)) {
                return true;
            }
        }
        return false;
    }

    //存放的是{xid -> stamp} xid属于哪个分支
    private Map<Long, Integer> xidStamp;
    private int stamp;

    //true:有环，false：没有环
    private boolean dfs(long xid) {
        Integer sta = xidStamp.get(xid);
        //如果当前xid已经属于某个图结构，且这个图结构恰好就是当前图结构，说明出现了环。
        if (sta != null && sta == stamp) {
            return true;
        }
        //如果当前xid已经属于某个图结构，且这个图结构是前面dfs出的某个图结构，说明没有环。
        if (sta != null && sta < stamp) {
            return false;
        }

        //如果当前xid不在某个图结构中，那么就以xid为根节点，dfs。也就是说当前的xid在一个独立的结构stamp中。
        xidStamp.put(xid, stamp);
        //当前xid正在等待的uid
        Long uid = waitU.get(xid);
        //如果当前xid没有等待的uid，那么肯定没有环。
        if(uid == null) return false;
        //持有这个uid的xid是x
        Long x = u2x.get(uid);
        return dfs(x);
    }

    /**
     * 将id1加入到key为id1对应的value中,新加入的uid1放到列表index为0的位置。
     */
    private void putIntoList(Map<Long, List<Long>> x2u, long id0, long id1) {
        if (!x2u.containsKey(id0)) {
            x2u.put(id0, new ArrayList<>());
        }
        x2u.get(id0).add(0, id1);
    }

    /**
     * @Describe 判断uid是否在xid对应的value中。等价于xid是否已经持有了资源uid。
     * @return true:在。false：不在。
     */
    private boolean isInList(Map<Long, List<Long>> x2u, long xid, long uid) {
        if (!x2u.containsKey(xid)) return false;
        return x2u.get(xid).contains(uid);
    }
}
