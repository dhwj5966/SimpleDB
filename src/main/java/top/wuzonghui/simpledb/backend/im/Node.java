package top.wuzonghui.simpledb.backend.im;

import top.wuzonghui.simpledb.backend.common.SubArray;
import top.wuzonghui.simpledb.backend.dm.dataitem.DataItem;
import top.wuzonghui.simpledb.backend.tm.TransactionManagerImpl;
import top.wuzonghui.simpledb.backend.utils.Parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author Starry
 * @create 2023-01-04-1:49 PM
 * @Describe
 * @Detail Node结构如下：
 * [LeafFlag,1byte][KeyNumber,2byte][SiblingUid,8byte]
 * [Son0][Key0][Son1][Key1]...[SonN][KeyN]
 * 1.LeafFlag标记了该节点是否是叶子节点。
 * 2.KeyNumber为该节点中key的个数。
 * 3.SiblingUid是其兄弟节点所在的DataItem的uid。
 */
public class Node {
    static final int IS_LEAF_OFFSET = 0;
    static final int NO_KEYS_OFFSET = IS_LEAF_OFFSET + 1;
    static final int SIBLING_OFFSET = NO_KEYS_OFFSET + 2;
    static final int NODE_HEADER_SIZE = SIBLING_OFFSET + 8;

    static final int BALANCE_NUMBER = 32;
    static final int NODE_SIZE = NODE_HEADER_SIZE + (2 * 8) * (BALANCE_NUMBER * 2 + 2);

    /**
     * 该节点所处的B+树对象。
     */
    BPlusTree tree;

    /**
     * Node所处的DataItem。
     */
    DataItem dataItem;

    /**
     * 实际存储Node数据的SubArray，就是所处DataItem的Data部分。
     */
    SubArray raw;

    /**
     * node的uid，也是所处dataItem的uid。
     */
    long uid;

    //根据isLeaf参数，设置raw的第一位是0还是1。
    static void setRawIsLeaf(SubArray raw, boolean isLeaf) {
        if (isLeaf) {
            raw.raw[raw.start + IS_LEAF_OFFSET] = (byte) 1;
        } else {
            raw.raw[raw.start + IS_LEAF_OFFSET] = (byte) 0;
        }
    }

    /**
     * @Describe raw代表的node是否是叶子节点。
     * @param raw SubArray对象，必须对应一个node。
     * 格式为[LeafFlag,1byte][KeyNumber,2byte][SiblingUid,8byte]
     * [Son0][Key0][Son1][Key1]...[SonN][KeyN]。
     * @return true:该raw代表的node是叶子节点，该raw代表的node节点不是叶子节点。
     * @Detail 通过解析raw的指定位置的数据，判断是否是叶子节点。
     */
    static boolean getRawIfLeaf(SubArray raw) {
        return raw.raw[raw.start + IS_LEAF_OFFSET] == (byte) 1;
    }

    //设置raw的KeyNumber位为noKeys
    static void setRawNoKeys(SubArray raw, int noKeys) {
        System.arraycopy(Parser.short2Byte((short) noKeys), 0, raw.raw, raw.start + NO_KEYS_OFFSET, 2);
    }

    /**
     * 根据raw，解析raw中的KeyNumber部分。
     * @param raw 待解析的SubArray对象，需要是一个Node的格式。
     * @return 返回raw对应的node共含有多少Son。
     */
    static int getRawNoKeys(SubArray raw) {
        return (int) Parser.parseShort(Arrays.copyOfRange(raw.raw, raw.start + NO_KEYS_OFFSET, raw.start + NO_KEYS_OFFSET + 2));
    }

    //
    static void setRawSibling(SubArray raw, long sibling) {
        System.arraycopy(Parser.long2Byte(sibling), 0, raw.raw, raw.start + SIBLING_OFFSET, 8);
    }

    /**
     * 解析raw的SiblingUid部分。
     * @param raw SubArray对象，需要是node的格式。
     * @return raw的SiblingUid部分，该表raw对应的node的兄弟节点的uid。
     */
    static long getRawSibling(SubArray raw) {
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, raw.start + SIBLING_OFFSET, raw.start + SIBLING_OFFSET + 8));
    }

    //设置raw的第kth个son的son为uid，从0开始。
    static void setRawKthSon(SubArray raw, long uid, int kth) {
        int offset = raw.start + NODE_HEADER_SIZE + kth * (8 * 2);
        System.arraycopy(Parser.long2Byte(uid), 0, raw.raw, offset, 8);
    }

    //
    static long getRawKthSon(SubArray raw, int kth) {
        int offset = raw.start + NODE_HEADER_SIZE + kth * (8 * 2);
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, offset, offset + 8));
    }

    //设置raw的第kth个son的key为uid，从0开始。
    static void setRawKthKey(SubArray raw, long key, int kth) {
        int offset = raw.start + NODE_HEADER_SIZE + kth * (8 * 2) + 8;
        System.arraycopy(Parser.long2Byte(key), 0, raw.raw, offset, 8);
    }

    /**
     * 获取raw的第kth个key，kth从0开始。
     * @param raw
     * @param kth
     * @return
     */
    static long getRawKthKey(SubArray raw, int kth) {
        int offset = raw.start + NODE_HEADER_SIZE + kth * (8 * 2) + 8;
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, offset, offset + 8));
    }

    static void copyRawFromKth(SubArray from, SubArray to, int kth) {
        int offset = from.start + NODE_HEADER_SIZE + kth * (8 * 2);
        System.arraycopy(from.raw, offset, to.raw, to.start + NODE_HEADER_SIZE, from.end - offset);
    }

    static void shiftRawKth(SubArray raw, int kth) {
        int begin = raw.start + NODE_HEADER_SIZE + (kth + 1) * (8 * 2);
        int end = raw.start + NODE_SIZE - 1;
        for (int i = end; i >= begin; i--) {
            raw.raw[i] = raw.raw[i - (8 * 2)];
        }
    }

    /**
     * 生成一个根节点的数据，该根节点的2个初始子节点分别是left和right，初始键值为key。
     * @param left 初始左子节点。
     * @param right 初始右子节点。
     * @param key
     * @return 生成的根节点的数据。
     */
    static byte[] newRootRaw(long left, long right, long key) {
        //新建一个SubArray，长度为node的长度。
        SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);
        //初始化前三个字段。
        setRawIsLeaf(raw, false);
        setRawNoKeys(raw, 2);
        setRawSibling(raw, 0);
        //第0个子节点是left。
        setRawKthSon(raw, left, 0);
        //key0是key。
        setRawKthKey(raw, key, 0);

        //第1个子节点是right。
        setRawKthSon(raw, right, 1);

        //key2是Long.MAX_VALUE
        setRawKthKey(raw, Long.MAX_VALUE, 1);

        return raw.raw;
    }

    /**
     * 生成一个空的节点的数据。
     * 该节点是叶子节点，且没有任何的son，也没有兄弟节点。
     * @return 空节点的数据。
     */
    static byte[] newNilRootRaw() {
        SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);

        setRawIsLeaf(raw, true);
        //该节点没有key。
        setRawNoKeys(raw, 0);
        //该节点没有兄弟节点。
        setRawSibling(raw, 0);
        return raw.raw;
    }

    /**
     * 根据B+树和node的uid，加载node。
     * @param bTree 节点所处的B树。
     * @param uid 节点的uid。
     * @return 加载出的Node对象。
     * @throws Exception
     */
    static Node loadNode(BPlusTree bTree, long uid) throws Exception {
        /*
            提示：根据b树中的dm，和uid，读取DataItem
         */
        DataItem di = bTree.dm.read(uid);
        assert di != null;
        Node n = new Node();
        n.tree = bTree;
        n.dataItem = di;
        n.raw = di.data();
        n.uid = uid;
        return n;
    }

    /**
     * @Describe 释放该节点。
     * release一个node就是release其所处的dataItem。
     */
    public void release() {
        dataItem.release();
    }

    /**
     * 判断该node是否是一个叶子节点。
     * @return true:是叶子节点。false：不是叶子节点。
     */
    public boolean isLeaf() {
        dataItem.rLock();
        try {
            return getRawIfLeaf(raw);
        } finally {
            dataItem.rUnLock();
        }
    }


    class SearchNextRes {
        long uid;
        long siblingUid;
    }

    /**
     * 找到从该节点出发，如果要寻找指定key，下一步要搜寻的节点。
     * @param key 要搜索的key。
     * @return 返回下一步要搜索的节点的uid。
     * 返回一个SearchNextRes对象，该对象有2个field，long uid和long siblingUid。
     * 如果返回结果的siblingUid字段为0，说明key在当前节点的子节点中，SearchNextRes对象的uid字段即下一步要搜寻的子节点的uid。
     * 如果返回结果的siblingUid字段不为0，说明key在当前节点的兄弟节点中，SearchNextRes对象的siblingUid字段即兄弟节点的uid。
     * @Instance 假设当前node的son-key部分存有以下值：uid1,7; uid2,11; uid3,20。node的兄弟节点的uid为uid4。
     * 1.调用searchNext(8)，则返回uid2。
     * 2.调用searchNext(23),则会返回uid4。
     */
    public SearchNextRes searchNext(long key) {
        //1.由于要读dataItem的数据，因此上读锁。
        dataItem.rLock();
        try {
            SearchNextRes res = new SearchNextRes();
            //解析当前节点共有多少个son-key对。
            int noKeys = getRawNoKeys(raw);
            //遍历这些son-key对
            for (int i = 0; i < noKeys; i++) {
                //遍历ik并和参数key比较，一旦key < ik，说明下一个node即ison
                long ik = getRawKthKey(raw, i);
                if (key < ik) {
                    res.uid = getRawKthSon(raw, i);
                    res.siblingUid = 0;
                    return res;
                }
            }
            //如果遍历完也没返回，则说明要去兄弟节点找。
            res.uid = 0;
            res.siblingUid = getRawSibling(raw);
            return res;

        } finally {
            dataItem.rUnLock();
        }
    }

    class LeafSearchRangeRes {
        List<Long> uids;
        long siblingUid;
    }

    /**
     * 在当前节点进行范围查找，范围是[leftKey, rightKey]。返回所有满足条件的uid。
     * 注意：约定如果rightKey大于等于该节点的最大key，则同时返回兄弟节点的uid，方便继续在下一个node中搜索。
     * @param leftKey 查询的key的左边界。
     * @param rightKey 查询的key的右边界。
     * @return LeafSearchRangeRes对象，内部封装了List<Long> uids和long siblingUid。
     * 如果rightKey大于等于该节点的最大key，则siblingUid指明了兄弟节点的uid，否则则为0。
     */
    public LeafSearchRangeRes leafSearchRange(long leftKey, long rightKey) {
        //由于涉及从dataItem中读数据，因此需要上读锁。
        dataItem.rLock();
        try {
            //当前node一共有noKeys个key。
            int noKeys = getRawNoKeys(raw);
            //不断遍历key，直到找到大于等于左边界leftKey的key。
            int kth = 0;
            while (kth < noKeys) {
                long ik = getRawKthKey(raw, kth);
                if (ik >= leftKey) {
                    break;
                }
                kth++;
            }
            //继续遍历key，当key小于等于右边界rightKey时，不断将key对应的son的uid加入list中。
            List<Long> uids = new ArrayList<>();
            while (kth < noKeys) {
                long ik = getRawKthKey(raw, kth);
                if (ik <= rightKey) {
                    uids.add(getRawKthSon(raw, kth));
                    kth++;
                } else {
                    break;
                }
            }
            //边界判断，如果已经遍历到最后一个key了，说明可能还可以继续看兄弟节点，因此把兄弟节点的uid也放到放回结果里。
            long siblingUid = 0;
            if (kth == noKeys) {
                siblingUid = getRawSibling(raw);
            }
            LeafSearchRangeRes res = new LeafSearchRangeRes();
            res.uids = uids;
            res.siblingUid = siblingUid;
            return res;
        } finally {
            //释放读锁。
            dataItem.rUnLock();
        }
    }

    class InsertAndSplitRes {
        long siblingUid, newSon, newKey;
    }

    /**
     * 将指定的son-key对插入到该node中。
     * @param uid son
     * @param key key
     * @return InsertAndSplitRes对象，包括三个字段：long siblingUid, newSon, newKey。
     * @throws Exception
     */
    public InsertAndSplitRes insertAndSplit(long uid, long key) throws Exception {
        boolean success = false;
        Exception err = null;
        InsertAndSplitRes res = new InsertAndSplitRes();

        dataItem.before();
        try {
            //如果没插入成功，那么说明应该插入到兄弟节点中，因此将兄弟节点封装到结果集中返回。
            success = insert(uid, key);
            if (!success) {
                res.siblingUid = getRawSibling(raw);
                return res;
            }
            //如果插入后，当前node的阶数不对，说明需要分裂。
            if (needSplit()) {
                try {
                    SplitRes r = split();
                    res.newSon = r.newSon;
                    res.newKey = r.newKey;
                    return res;
                } catch (Exception e) {
                    err = e;
                    throw e;
                }
            } else {
                return res;
            }
        } finally {
            if (err == null && success) {
                dataItem.after(TransactionManagerImpl.SUPER_XID);
            } else {
                dataItem.unBefore();
            }
        }
    }

    /**
     * 将son-key对插入到当前节点中。
     * @param uid son
     * @param key key
     * @return 是否插入成功。
     */
    private boolean insert(long uid, long key) {
        //获取当前节点共有多少son-key对。
        int noKeys = getRawNoKeys(raw);
        //遍历当前节点的这些son-key对，寻找应该插入的位置。
        int kth = 0;
        while (kth < noKeys) {
            long ik = getRawKthKey(raw, kth);
            if (ik < key) {
                kth++;
            } else {
                break;
            }
        }
        //如果已经遍历完了依然没有找到合适的位置，且该节点还有下一个节点，则返回false。也就是说如果当前节点已经是最后一个节点了，那么就不返回false。
        if (kth == noKeys && getRawSibling(raw) != 0) return false;

        if (getRawIfLeaf(raw)) {
            //如果是叶子节点。
            //横移kth组后面的son-key
            shiftRawKth(raw, kth);
            //设置第kth组son-key对
            setRawKthKey(raw, key, kth);
            setRawKthSon(raw, uid, kth);
            //更新维护一下NoKey部分
            setRawNoKeys(raw, noKeys + 1);
        } else {
            //如果不是叶子节点。
            //获取第kth组key，记为kk
            long kk = getRawKthKey(raw, kth);
            setRawKthKey(raw, key, kth);
            shiftRawKth(raw, kth + 1);
            setRawKthKey(raw, kk, kth + 1);
            setRawKthSon(raw, uid, kth + 1);
            setRawNoKeys(raw, noKeys + 1);
        }
        return true;
    }

    /**
     * @Describe 当前节点是否需要分裂。
     * @Detail 根据当前存储的son-key对是否达到 BALANCE_NUMBER * 2来判断。
     * @return true:需要分裂。 false:不需要分裂。
     */
    private boolean needSplit() {
        return BALANCE_NUMBER * 2 == getRawNoKeys(raw);
    }

    class SplitRes {
        long newSon, newKey;
    }

    /**
     * 分裂。
     * @return
     * @throws Exception
     */
    private SplitRes split() throws Exception {
        SubArray nodeRaw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);
        setRawIsLeaf(nodeRaw, getRawIfLeaf(raw));
        setRawNoKeys(nodeRaw, BALANCE_NUMBER);
        setRawSibling(nodeRaw, getRawSibling(raw));
        copyRawFromKth(raw, nodeRaw, BALANCE_NUMBER);
        long son = tree.dm.insert(TransactionManagerImpl.SUPER_XID, nodeRaw.raw);
        setRawNoKeys(raw, BALANCE_NUMBER);
        setRawSibling(raw, son);

        SplitRes res = new SplitRes();
        res.newSon = son;
        res.newKey = getRawKthKey(nodeRaw, 0);
        return res;
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Is leaf: ").append(getRawIfLeaf(raw)).append("\n");
        int KeyNumber = getRawNoKeys(raw);
        sb.append("KeyNumber: ").append(KeyNumber).append("\n");
        sb.append("sibling: ").append(getRawSibling(raw)).append("\n");
        for (int i = 0; i < KeyNumber; i++) {
            sb.append("son: ").append(getRawKthSon(raw, i)).append(", key: ").append(getRawKthKey(raw, i)).append("\n");
        }
        return sb.toString();
    }

}
