package top.wuzonghui.simpledb.backend.im;

import top.wuzonghui.simpledb.backend.common.SubArray;
import top.wuzonghui.simpledb.backend.dm.DataManager;
import top.wuzonghui.simpledb.backend.dm.dataitem.DataItem;
import top.wuzonghui.simpledb.backend.tm.TransactionManagerImpl;
import top.wuzonghui.simpledb.backend.utils.Parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Starry
 * @create 2023-01-04-1:45 PM
 * @Describe B+树。
 */
public class BPlusTree {
    /**
     * B+树的Node是基于DataItem实现的，所以必须依赖DataManager对象。
     */
    DataManager dm;

    /**
     * 该B+树的bootDataItem的uid。
     */
    long bootUid;

    /**
     * 由于B+树会动态调整树结构，因此Root节点不固定。因此用该bootDataItem存储根节点的uid。
     * 这种思想类似于链表问题的dummy node。
     */
    DataItem bootDataItem;

    /**
     * 锁对象，用以在操作bootDataItem的时候，保证数据一致性。
     * 可能有多个事务同时访问B+树，但bootDataItem的操作必须保证是穿行的。
     */
    Lock bootLock;

    /**
     * 创建一颗B+树。
     * @param dm DataManager对象。
     * @return 返回该B+树的bootDataItem的uid。
     * @throws Exception
     */
    public static long create(DataManager dm) throws Exception {
        //新建一个node的byte数组。[LeafFlag,1byte][KeyNumber,2byte][SiblingUid,8byte]
        byte[] rawRoot = Node.newNilRootRaw();
        //以超级事务,将rawRoot插入到数据库中，即B+树的RootNode，得到RootNode所在DataItem的uid。
        long rootUid = dm.insert(TransactionManagerImpl.SUPER_XID, rawRoot);
        //将RootNode所在DataItem的uid插入到数据库中，即B+树的bootDataItem，返回该dataItem的uid。
        return dm.insert(TransactionManagerImpl.SUPER_XID, Parser.long2Byte(rootUid));
    }


    /**
     * 通过bootDataItem的uid和DataManager对象，加载一颗B+树。
     * @param bootUid bootDataItem的uid
     * @param dm DataManager对象
     * @return 加载出的B+树对象。
     * @throws Exception
     */
    public static BPlusTree load(long bootUid, DataManager dm) throws Exception {
        DataItem bootDataItem = dm.read(bootUid);
        assert bootDataItem != null;
        BPlusTree t = new BPlusTree();
        t.bootUid = bootUid;
        t.dm = dm;
        t.bootDataItem = bootDataItem;
        t.bootLock = new ReentrantLock();
        return t;
    }

    /**
     * 获取rootNode的uid。
     * @return uid of rootNode。
     */
    private long rootUid() {
        /*
            提示：从bootDataItem里解析即可，读bootDataItem也需要上锁。
         */
        bootLock.lock();
        try {
            SubArray sa = bootDataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start, sa.start + 8));
        } finally {
            bootLock.unlock();
        }
    }


    /**
     * 根据指定key，搜索数据。
     * @param key 指定的key。
     * @return List<Long> uids，存储uid。
     * @throws Exception
     */
    public List<Long> search(long key) throws Exception {
        return searchRange(key, key);
    }

    /**
     * 范围搜索，范围为key属于[leftKey, rightKey]。
     * @param leftKey 左范围。
     * @param rightKey 右范围。
     * @return List<Long> uids，存储了所有在范围里的数据的uid。
     * @throws Exception
     */
    public List<Long> searchRange(long leftKey, long rightKey) throws Exception {
        //获取rootNode的uid。
        long rootUid = rootUid();
        //找到leftKey所在的叶子节点的uid。
        long leafUid = searchLeaf(rootUid, leftKey);
        List<Long> uids = new ArrayList<>();
        while (true) {
            Node leaf = Node.loadNode(this, leafUid);
            Node.LeafSearchRangeRes res = leaf.leafSearchRange(leftKey, rightKey);
            leaf.release();
            uids.addAll(res.uids);
            if (res.siblingUid == 0) {
                break;
            } else {
                leafUid = res.siblingUid;
            }
        }
        return uids;
    }

    /**
     * @Describe 向该B+树中插入son,key组合。
     * @param key 待插入的key。
     * @param uid 待插入的son。(uid)
     * @throws Exception
     */
    public void insert(long key, long uid) throws Exception {
        //获取rootNode的uid。
        long rootUid = rootUid();
        InsertRes res = insert(rootUid, uid, key);
        assert res != null;
        if (res.newNode != 0) {
            updateRootUid(rootUid, res.newNode, res.newKey);
        }
    }

    public void close() {
        bootDataItem.release();
    }


    /**
     * @Describe 更新bootDataItem中根节点的uid。因为更新RootNode，一定是RootNode分裂，所以一定是有两个子节点。
     * @param left 根节点的左子节点。
     * @param right 根节点的右子节点。
     * @param rightKey
     * @throws Exception
     */
    private void updateRootUid(long left, long right, long rightKey) throws Exception {
        bootLock.lock();
        try {
            //生成新的根节点的数据的byte数组。
            byte[] rootRaw = Node.newRootRaw(left, right, rightKey);
            //将byte数组插入到dataItem中。得到该dataItem的uid。
            long newRootUid = dm.insert(TransactionManagerImpl.SUPER_XID, rootRaw);
            //将bootDataItem的data改为新的uid。
            bootDataItem.before();
            SubArray diRaw = bootDataItem.data();
            System.arraycopy(Parser.long2Byte(newRootUid), 0, diRaw.raw, diRaw.start, 8);
            bootDataItem.after(TransactionManagerImpl.SUPER_XID);
        } finally {
            bootLock.unlock();
        }
    }

    /**
     * 从指定node出发，根据key找到叶子节点。
     * @param nodeUid node的uid。
     * @param key 通过key定位路线。
     * @return 根据key找到的叶子节点的uid。
     * @throws Exception
     */
    private long searchLeaf(long nodeUid, long key) throws Exception {
        //加载出node。
        Node node = Node.loadNode(this, nodeUid);
        boolean isLeaf = node.isLeaf();
        node.release();

        if (isLeaf) {
            //如果该节点是叶子节点，那简单了,直接返回该节点的uid即可。
            return nodeUid;
        } else {
            //如果该节点不是叶子节点，就要递归往下找直到找到叶子节点，往当前节点的哪个子节点找取决于key。
            //next是下一个要找的节点。
            long next = searchNext(nodeUid, key);
            return searchLeaf(next, key);
        }
    }

    /**
     * 从指定node出发，返回要搜索指定key，下一步需要搜寻的node的uid。
     * @param nodeUid 从指定的node出发。
     * @param key 搜索指定key
     * @return 下一步要搜寻的node的uid。
     * @throws Exception
     */
    private long searchNext(long nodeUid, long key) throws Exception {
        while (true) {
            Node node = Node.loadNode(this, nodeUid);
            Node.SearchNextRes res = node.searchNext(key);
            node.release();
            //如果res的uid不等于0，则说明找到了下一步搜索的node，返回。
            if (res.uid != 0) return res.uid;
            //如果uid=0，则说明下一步搜索的node是当前节点的兄弟节点。
            nodeUid = res.siblingUid;
        }
    }

    class InsertRes {
        long newNode, newKey;
    }


    /**
     * 在指定Node的位置，插入son-key，如果指定节点不是叶子节点，就往下搜索直到搜索到叶子节点。
     * @param nodeUid 当前所处节点的uid。
     * @param uid 待插入的son(uid)。
     * @param key 待插入的key。
     * @return InsertRes对象。
     * @throws Exception
     */
    private InsertRes insert(long nodeUid, long uid, long key) throws Exception {
        /*
            coding：根据nodeUid获取节点。
            如果是叶子节点，调用insertAndSplit。
            否则根据key找到下层并递归调用，直到找到叶子节点为止。
         */
        Node node = Node.loadNode(this, nodeUid);
        boolean isLeaf = node.isLeaf();
        node.release();

        InsertRes res = null;
        if (isLeaf) {
        //如果是叶子节点。
            res = insertAndSplit(nodeUid, uid, key);
        } else {
            //如果不是叶子节点，找到下一层的node的uid，递归。
            long next = searchNext(nodeUid, key);
            InsertRes ir = insert(next, uid, key);
            if (ir.newNode != 0) {
                res = insertAndSplit(nodeUid, ir.newNode, ir.newKey);
            } else {
                res = new InsertRes();
            }
        }
        return res;
    }

    /**
     * 插入并分裂。
     * @param nodeUid
     * @param uid
     * @param key
     * @return
     * @throws Exception
     */
    private InsertRes insertAndSplit(long nodeUid, long uid, long key) throws Exception {
        while (true) {
            Node node = Node.loadNode(this, nodeUid);
            Node.InsertAndSplitRes iasr = node.insertAndSplit(uid, key);
            node.release();

            if (iasr.siblingUid != 0) {
                nodeUid = iasr.siblingUid;
            } else {
                InsertRes res = new InsertRes();
                res.newNode = iasr.newSon;
                res.newKey = iasr.newKey;
                return res;
            }
        }
    }


}
