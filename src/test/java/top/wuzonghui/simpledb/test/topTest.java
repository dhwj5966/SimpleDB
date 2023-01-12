package top.wuzonghui.simpledb.test;

import org.junit.Test;
import top.wuzonghui.simpledb.backend.dm.DataManager;
import top.wuzonghui.simpledb.backend.server.Executor;
import top.wuzonghui.simpledb.backend.tbm.TableManager;
import top.wuzonghui.simpledb.backend.tm.TransactionManager;
import top.wuzonghui.simpledb.backend.vm.VersionManager;
import top.wuzonghui.simpledb.backend.vm.VersionManagerImpl;

/**
 * @author Starry
 * @create 2023-01-11-9:01 PM
 * @Describe
 */
public class topTest {
    @Test
    public void createDB() {
        String path = "C:\\Users\\windows\\Desktop\\test";
        createDB(path);
    }

    @Test
    public void test() throws Exception {
        String path = "C:\\Users\\windows\\Desktop\\test";
        Executor executor = openDB(path);
        /*
            bug汇总：
            1.begin一个事务后，还没commit就直接关闭程序，则属于异常关闭，需要走恢复例程，此后再select *,会报错。
         */


        String[] sqls = {
//                "create table users id int32,name string(index id name)",
//                "begin",
//                "insert into users values 6 \"wyy\"",
                "select * from users"

        };

        for (String sql : sqls) {
            execute0(executor, sql);
        }


    }

    private void execute0(Executor executor, String sql) throws Exception {
        byte[] execute = executor.execute(sql.getBytes());
        System.out.println(new String(execute));
    }

    public Executor openDB(String path) {
        TransactionManager tm = TransactionManager.open(path);
        DataManager dm = DataManager.open(path, (1 << 20) * 64, tm);
        VersionManager vm = new VersionManagerImpl(tm, dm);
        TableManager tbm = TableManager.open(path, vm, dm);
        return new Executor(tbm);
    }

    public void createDB(String path) {
        TransactionManager tm = TransactionManager.create(path);
        DataManager dm = DataManager.create(path, (1 << 20) * 64, tm);
        VersionManager vm = VersionManager.newVersionManager(tm, dm);
        TableManager.create(path, vm, dm);

        tm.close();
        dm.close();
    }
}
