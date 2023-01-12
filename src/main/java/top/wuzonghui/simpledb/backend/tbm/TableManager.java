package top.wuzonghui.simpledb.backend.tbm;

import top.wuzonghui.simpledb.backend.dm.DataManager;
import top.wuzonghui.simpledb.backend.parser.parser.statement.*;
import top.wuzonghui.simpledb.backend.utils.Parser;
import top.wuzonghui.simpledb.backend.vm.VersionManager;

/**
 * @author Starry
 * @create 2023-01-06-9:37 PM
 * @Describe
 */
public interface TableManager {
    /**
     * @Describe 调用vm开启一个事务，并将xid和"begin".getByte()封装到BeginRes对象中返回。
     * @param begin Begin对象。
     * @return BeginRes对象。
     */
    BeginRes begin(Begin begin);

    /**
     * 调用vm提交一个事务，并将"commit".getBytes()返回。
     * @param xid
     * @return
     * @throws Exception
     */
    byte[] commit(long xid) throws Exception;

    /**
     * 回滚一个事务，调用vm实现，并将"abort".getBytes()返回
     * @param xid
     * @return
     */
    byte[] abort(long xid);

    /**
     * 返回所有表信息。
     * @param xid
     */
    byte[] show(long xid);

    /**
     * 指定事务创建表。
     */
    byte[] create(long xid, Create create) throws Exception;

    /**
     * 指定事务插入数据。
     */
    byte[] insert(long xid, Insert insert) throws Exception;

    /**
     * 指定事务read数据。
     * @return read的结果是String类型，将该String类型getBytes()即返回的字节数组。
     */
    byte[] read(long xid, Select select) throws Exception;
    byte[] update(long xid, Update update) throws Exception;
    byte[] delete(long xid, Delete delete) throws Exception;

    public static TableManager create(String path, VersionManager vm, DataManager dm) {
        Booter booter = Booter.create(path);
        booter.update(Parser.long2Byte(0));
        return new TableManagerImpl(vm, dm, booter);
    }

    public static TableManager open(String path, VersionManager vm, DataManager dm) {
        Booter booter = Booter.open(path);
        return new TableManagerImpl(vm, dm, booter);
    }

    /**
     * 删除表要符合一下规则，只要有其他事务对表有任何形式的占用，则无法删除表。
     * 什么叫其他事务对表的占用？即
     *
     * @param xid
     * @param stat
     * @return
     */
    byte[] drop(long xid, Drop stat);
}
