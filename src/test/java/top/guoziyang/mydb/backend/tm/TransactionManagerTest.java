package top.guoziyang.mydb.backend.tm;

import org.junit.Test;
import top.wuzonghui.simpledb.backend.tm.TransactionManager;

import java.io.File;
import java.security.SecureRandom;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * TransactionManager的单元测试
 */
public class TransactionManagerTest {



    static Random random = new SecureRandom();

    private int transCnt = 0;
    private int noWorkers = 50;
    private int noWorks = 3000;
    private Lock lock = new ReentrantLock();
    private TransactionManager tmger;
    private Map<Long, Byte> transMap;
    private CountDownLatch cdl;

    @Test
    public void testMultiThread() {
        //调用TransactionManager类的create静态方法，创建xid文件，并获取TransactionManager对象。
        tmger = TransactionManager.create("C:\\Users\\windows\\Desktop\\tranmger_test");
        transMap = new ConcurrentHashMap<>();
        cdl = new CountDownLatch(noWorkers);
        //开启多线程
        for(int i = 0; i < noWorkers; i ++) {
            Runnable r = () -> worker();
            new Thread(r).run();
        }
        try {
            cdl.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //断言：xid文件可以成功删除
        tmger.close();
        boolean ff = new File("C:\\Users\\windows\\Desktop\\tranmger_test.xid").delete();
        assert ff;
    }


    private void worker() {
        boolean inTrans = false;
        long transXID = 0;
        //每个线程循环3000次
        for(int i = 0; i < noWorks; i ++) {
            int op = Math.abs(random.nextInt(6));
            //  1/6的概率触发
            if(op == 0) {
                lock.lock();
                if(inTrans == false) {
                    //新建一个事务
                    long xid = tmger.begin();
                    //在map中存储 xid -> 0
                    transMap.put(xid, (byte)0);
                    //计数器+1
                    transCnt ++;
                    transXID = xid;
                    inTrans = true;
                } else {
                    //随机决定将事务回滚还是提交
                    int status = (random.nextInt(Integer.MAX_VALUE) % 2) + 1;
                    switch(status) {
                        case 1:
                            tmger.commit(transXID);
                            break;
                        case 2:
                            tmger.abort(transXID);
                            break;
                    }
                    transMap.put(transXID, (byte)status);
                    inTrans = false;
                }
                lock.unlock();
            } else {
                //  5/6的概率触发
                lock.lock();
                if(transCnt > 0) {
                    //从已经获取到的事务中，随便选一个，取得对应的xid
                    long xid = (long)((random.nextInt(Integer.MAX_VALUE) % transCnt) + 1);
                    //从map中取得该xid对应的status，如果status和事务的状态相符
                    byte status = transMap.get(xid);
                    boolean ok = false;
                    switch (status) {
                        case 0:
                            ok = tmger.isActive(xid);
                            break;
                        case 1:
                            ok = tmger.isCommitted(xid);
                            break;
                        case 2:
                            ok = tmger.isAborted(xid);
                            break;
                    }
                    assert ok;
                }
                lock.unlock();
            }
        }
        cdl.countDown();
    }
}
