package top.wuzonghui.simpledb.backend.im;

import org.junit.Test;
import top.wuzonghui.simpledb.backend.dm.DataManager;
import top.wuzonghui.simpledb.backend.dm.pagecache.PageCache;
import top.wuzonghui.simpledb.backend.tm.MockTransactionManager;
import top.wuzonghui.simpledb.backend.tm.TransactionManager;


import java.io.File;
import java.util.List;

public class BPlusTreeTest {
    @Test
    public void testTreeSingle() throws Exception {
        TransactionManager tm = new MockTransactionManager();
        DataManager dm = DataManager.create("C:\\Users\\windows\\Desktop\\TestTreeSingle", PageCache.PAGE_SIZE*10, tm);

        long root = BPlusTree.create(dm);
        BPlusTree tree = BPlusTree.load(root, dm);

        int lim = 10000;
        for(int i = lim-1; i >= 0; i --) {
            tree.insert(i, i);
        }

        for(int i = 0; i < lim; i ++) {
            List<Long> uids = tree.search(i);
            assert uids.size() == 1;
            assert uids.get(0) == i;
        }

        dm.close();
        tm.close();
        assert new File("C:\\Users\\windows\\Desktop\\TestTreeSingle.db").delete();
        assert new File("C:\\Users\\windows\\Desktop\\TestTreeSingle.log").delete();
    }
}
