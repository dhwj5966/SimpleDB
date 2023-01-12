package top.wuzonghui.simpledb.backend.tbm;

import top.wuzonghui.simpledb.backend.dm.DataManager;
import top.wuzonghui.simpledb.backend.parser.parser.statement.*;
import top.wuzonghui.simpledb.backend.utils.Parser;
import top.wuzonghui.simpledb.backend.vm.Transaction;
import top.wuzonghui.simpledb.backend.vm.VersionManager;
import top.wuzonghui.simpledb.common.Error;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Starry
 * @create 2023-01-07-1:49 AM
 * @Describe
 */
public class TableManagerImpl implements TableManager {
    /**
     * TBM依赖的VM对象。
     */
    VersionManager vm;

    /**
     * TBM依赖的dm对象。
     */
    DataManager dm;

    /**
     *
     */
    private Booter booter;

    /**
     * Table对象的缓存，key为tablename，value为Table对象。在创建TableManagerImpl对象的时候，就会根据booter对象，将所有的Table加载到该Map中。
     */
    private Map<String, Table> tableCache;


    private Map<Long, List<Table>> xidTableCache;

    private Lock lock;

    TableManagerImpl(VersionManager vm, DataManager dm, Booter booter) {
        this.vm = vm;
        this.dm = dm;
        this.booter = booter;
        this.tableCache = new HashMap<>();
        this.xidTableCache = new HashMap<>();
        lock = new ReentrantLock();
        loadTables();
    }

    @Override
    public BeginRes begin(Begin begin) {
        int level = begin.isRepeatableRead ? Transaction.REPEATABLEREAD : Transaction.READCOMMITTED;
        long xid = vm.begin(level);
        BeginRes beginRes = new BeginRes();
        beginRes.xid = xid;
        beginRes.result = "begin".getBytes();
        return beginRes;
    }

    @Override
    public byte[] commit(long xid) throws Exception {
        vm.commit(xid);
        return "commit".getBytes();
    }

    @Override
    public byte[] abort(long xid) {
        vm.abort(xid);
        return "abort".getBytes();
    }


    @Override
    public byte[] show(long xid) {
        lock.lock();
        try {
            StringBuilder sb = new StringBuilder();
            for (Table tb : tableCache.values()) {
                sb.append(tb.toString()).append("\n");
            }
//            List<Table> t = xidTableCache.get(xid);
//            if(t == null) {
//                return "\n".getBytes();
//            }
//            for (Table tb : t) {
//                sb.append(tb.toString()).append("\n");
//            }
            return sb.toString().getBytes();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public byte[] create(long xid, Create create) throws Exception {
        lock.lock();
        try {
            //如果该表已经在缓存中了，说明重复创建了，报错。
            if (tableCache.containsKey(create.tableName)) {
                throw Error.DuplicatedTableException;
            }
            //创建表，并更新booter中的数据。头插法。不要忘了更新tableCache。
            Table table = Table.createTable(this, firstTableUid(), xid, create);
            updateFirstTableUid(table.uid);
            tableCache.put(create.tableName, table);

            //xidTable存放的是一个xid创建的表的列表。比如{xid0 = [table1, table2.....]}
            if (!xidTableCache.containsKey(xid)) {
                xidTableCache.put(xid, new ArrayList<>());
            }
            xidTableCache.get(xid).add(table);
            return ("create " + create.tableName).getBytes();
        } finally {
            lock.unlock();
        }
    }


    @Override
    public byte[] insert(long xid, Insert insert) throws Exception {
        lock.lock();
        String tableName = insert.tableName;
        Table table = tableCache.get(tableName);
        lock.unlock();

        if (table == null) {
            throw Error.TableNotFoundException;
        }

        table.insert(xid, insert);
        return "insert".getBytes();
    }

    @Override
    public byte[] read(long xid, Select select) throws Exception {
        lock.lock();
        Table table = tableCache.get(select.tableName);
        lock.unlock();
        if (table == null) {
            throw Error.TableNotFoundException;
        }
        return table.read(xid, select).getBytes();
    }


    @Override
    public byte[] update(long xid, Update update) throws Exception {
        lock.lock();
        Table table = tableCache.get(update.tableName);
        lock.unlock();
        if (table == null) {
            throw Error.TableNotFoundException;
        }
        int count = table.update(xid, update);
        return ("update " + count + " raw").getBytes();
    }

    @Override
    public byte[] delete(long xid, Delete delete) throws Exception {
        lock.lock();
        Table table = tableCache.get(delete.tableName);
        lock.unlock();
        if (table == null) {
            throw Error.TableNotFoundException;
        }
        int count = table.delete(xid, delete);
        return ("delete " + count + " raw").getBytes();
    }

    @Override
    public byte[] drop(long xid, Drop stat) {
        return new byte[0];
    }

    /**
     * 初始化TMB对象时调用，将所有Table对象加载到内存中。
     */
    private void loadTables() {
        long uid = firstTableUid();
        while (uid != 0) {
            Table table = Table.loadTable(this, uid);
            uid = table.nextUid;
            tableCache.put(table.name, table);
        }
    }

    /**
     * 获取第一张表的uid。
     *
     * @return
     */
    private long firstTableUid() {
        byte[] raw = booter.load();
        return Parser.parseLong(raw);
    }

    /**
     * 将boot文件中的数据改为uid。
     * @param uid
     */
    private void updateFirstTableUid(long uid) {
        booter.update(Parser.long2Byte(uid));
    }
}
