package top.wuzonghui.simpledb.backend.dm;

import org.junit.Test;
import top.wuzonghui.simpledb.backend.common.SubArray;
import top.wuzonghui.simpledb.backend.dm.dataitem.DataItem;
import top.wuzonghui.simpledb.backend.dm.pagecache.PageCache;
import top.wuzonghui.simpledb.backend.tm.MockTransactionManager;
import top.wuzonghui.simpledb.backend.tm.TransactionManager;
import top.wuzonghui.simpledb.backend.utils.Panic;
import top.wuzonghui.simpledb.backend.utils.RandomUtil;


import java.io.File;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DataManagerTest {

    static List<Long> uids0, uids1;
    static Lock uidsLock;

    static Random random = new SecureRandom();

    private void initUids() {
        uids0 = new ArrayList<>();
        uids1 = new ArrayList<>();
        uidsLock = new ReentrantLock();
    }

    @Test
    public void demo() {

    }


    @Test
    public void testDMSingle() throws Exception {
        /*
            1.通过指定路径，创建DM对象。
         */
        TransactionManager tm0 = new MockTransactionManager();
        DataManager dm0 = DataManager.create("C:\\Users\\windows\\Desktop\\TESTDMSingle", PageCache.PAGE_SIZE*10, tm0);
        DataManager mdm = MockDataManager.newMockDataManager();

        int tasksNum = 10000;
        CountDownLatch cdl = new CountDownLatch(1);
        initUids();
        Runnable r = () -> worker(dm0, mdm, tasksNum, 50, cdl);
        new Thread(r).run();
        cdl.await();
        dm0.close();
        mdm.close();

        new File("C:\\Users\\windows\\Desktop\\TESTDMSingle.db").delete();
        new File("C:\\Users\\windows\\Desktop\\TESTDMSingle.log").delete();
    }

    @Test
    public void testDMMulti() throws InterruptedException {
        TransactionManager tm0 = new MockTransactionManager();
        DataManager dm0 = DataManager.create("C:\\Users\\windows\\Desktop\\TestDMMulti", PageCache.PAGE_SIZE*10, tm0);
        DataManager mdm = MockDataManager.newMockDataManager();
        int tasksNum = 500;
        CountDownLatch cdl = new CountDownLatch(10);
        initUids();
        for(int i = 0; i < 10; i ++) {
            Runnable r = () -> worker(dm0, mdm, tasksNum, 50, cdl);
            new Thread(r).run();
        }
        cdl.await();
        dm0.close(); mdm.close();

        new File("C:\\Users\\windows\\Desktop\\TestDMMulti.db").delete();
        new File("C:\\Users\\windows\\Desktop\\TestDMMulti.log").delete();
    }

    @Test
    public void testRecoverySimple() throws InterruptedException {
        TransactionManager tm0 = TransactionManager.create("C:\\Users\\windows\\Desktop\\TestRecoverySimple");
        DataManager dm0 = DataManager.create("C:\\Users\\windows\\Desktop\\TestRecoverySimple", PageCache.PAGE_SIZE*30, tm0);
        DataManager mdm = MockDataManager.newMockDataManager();
        dm0.close();

        initUids();
        int workerNums = 10;
        for(int i = 0; i < 8; i ++) {
            System.out.println(i);
            //除第一次外，每次调用open方法都会进入Recover逻辑，因为没有调用dm0的close方法。
            dm0 = DataManager.open("C:\\Users\\windows\\Desktop\\TestRecoverySimple", PageCache.PAGE_SIZE * 10, tm0);
            CountDownLatch cdl = new CountDownLatch(workerNums);
            for(int k = 0; k < workerNums; k ++) {
                final DataManager dm = dm0;

                Runnable r = () -> worker(dm, mdm, 100, 50, cdl);

                new Thread(r).run();
            }
            cdl.await();
        }
        dm0.close(); mdm.close();

        new File("C:\\Users\\windows\\Desktop\\TestRecoverySimple.db").delete();
        new File("C:\\Users\\windows\\Desktop\\TestRecoverySimple.log").delete();
        new File("C:\\Users\\windows\\Desktop\\TestRecoverySimple.xid").delete();

    }

    @Test
    public void demo1() {
        TransactionManager tm = TransactionManager.create("C:\\Users\\windows\\Desktop\\TestRecoverySimple");
        DataManager dm = DataManager.create("C:\\Users\\windows\\Desktop\\TestRecoverySimple", PageCache.PAGE_SIZE*30, tm);
        dm.close();

        dm = DataManager.open("C:\\Users\\windows\\Desktop\\TestRecoverySimple", PageCache.PAGE_SIZE * 10, tm);
        dm = DataManager.open("C:\\Users\\windows\\Desktop\\TestRecoverySimple", PageCache.PAGE_SIZE * 10, tm);
        dm = DataManager.open("C:\\Users\\windows\\Desktop\\TestRecoverySimple", PageCache.PAGE_SIZE * 10, tm);
        dm = DataManager.open("C:\\Users\\windows\\Desktop\\TestRecoverySimple", PageCache.PAGE_SIZE * 10, tm);
        dm = DataManager.open("C:\\Users\\windows\\Desktop\\TestRecoverySimple", PageCache.PAGE_SIZE * 10, tm);


    }
    private void worker(DataManager dm0, DataManager dm1, int tasksNum, int insertRation, CountDownLatch cdl) {
        int dataLen = 60;
        try {
            //循环
            for(int i = 0; i < tasksNum; i ++) {
                int op = Math.abs(random.nextInt()) % 100;
                if(op < insertRation) {
                    //随机路线1
                    /*
                        1.随机生成一个长度为60的byte[]序列
                        2.2个DM分别insert数据。
                        3.将插入数据生成的uid返回
                     */
                    byte[] data = RandomUtil.randomBytes(dataLen);
                    long u0, u1 = 0;
                    try {
                        u0 = dm0.insert(0, data);
                    } catch (Exception e) {
                        continue;
                    }
                    try {
                        u1 = dm1.insert(0, data);
                    } catch(Exception e) {
                        Panic.panic(e);
                    }
                    uidsLock.lock();
                    uids0.add(u0);
                    uids1.add(u1);
                    uidsLock.unlock();
                } else {
                    //随机路线2
                    /*
                        1.如果存放uid的集合里没有数据，就跳过。
                        2.随机从uid集合里选出2个uid，u0和u1
                        3.通过u0和u1获取DataItem
                        4.修改DataItem的数据
                     */
                    uidsLock.lock();
                    if(uids0.size() == 0) {
                        uidsLock.unlock();
                        continue;
                    }
                    int tmp = Math.abs(random.nextInt()) % uids0.size();
                    long u0 = uids0.get(tmp);
                    long u1 = uids1.get(tmp);
                    DataItem data0 = null, data1 = null;
                    try {
                        data0 = dm0.read(u0);
                    } catch (Exception e) {
                        Panic.panic(e);
                        continue;
                    }
                    if(data0 == null) continue;
                    try {
                        data1 = dm1.read(u1);
                    } catch (Exception e) {}

                    data0.rLock(); data1.rLock();
                    SubArray s0 = data0.data(); SubArray s1 = data1.data();
                    assert Arrays.equals(Arrays.copyOfRange(s0.raw, s0.start, s0.end), Arrays.copyOfRange(s1.raw, s1.start, s1.end));
                    data0.rUnLock(); data1.rUnLock();

                    byte[] newData = RandomUtil.randomBytes(dataLen);
                    data0.before(); data1.before();
                    System.arraycopy(newData, 0, s0.raw, s0.start, dataLen);
                    System.arraycopy(newData, 0, s1.raw, s1.start, dataLen);
                    data0.after(0); data1.after(0);
                    data0.release(); data1.release();
                }
            }
        } finally {
            cdl.countDown();
        }
    }
}
