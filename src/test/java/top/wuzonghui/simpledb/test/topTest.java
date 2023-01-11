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

        executor.execute("create table users id int32, name string,(index id name)".getBytes());

        executor.execute("insert into users values 5 \"wzh\"".getBytes());

        byte[] execute = executor.execute("select name from users where id < 10".getBytes());
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
