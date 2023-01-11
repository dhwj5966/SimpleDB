package top.wuzonghui.simpledb.backend.server;

import top.wuzonghui.simpledb.backend.parser.parser.Parser;
import top.wuzonghui.simpledb.backend.parser.parser.statement.*;
import top.wuzonghui.simpledb.backend.tbm.BeginRes;
import top.wuzonghui.simpledb.backend.tbm.TableManager;
import top.wuzonghui.simpledb.backend.vm.Transaction;
import top.wuzonghui.simpledb.common.Error;

/**
 * @author Starry
 * @create 2023-01-08-5:58 PM
 * @Describe
 */
public class Executor {

    TableManager tbm;

    long xid;

    public Executor(TableManager tbm) {
        this.tbm = tbm;
        this.xid = 0;
    }

    /**
     * 根据sql语句，执行对应逻辑。
     * @param sql sql语句的字节数据。
     */
    public byte[] execute(byte[] sql) throws Exception {
        /*
            1.调用Parser.Parser方法，将sql解析成statement对象stat。
            2.根据stat对象所属的类别，执行不同的逻辑。如果是begin，commit，abort，则由本方法执行。
            3.如果不是，就丢给execute2.
         */
        System.out.println("Execute: " + new String(sql));
        Object stat = Parser.Parse(sql);
        if (Begin.class.isInstance(stat)) {
            if (xid != 0) {
                throw Error.NestedTransactionException;
            }
            Begin begin = (Begin) stat;
            BeginRes beginRes = tbm.begin(begin);
            this.xid = beginRes.xid;
            return beginRes.result;
        } else if (Commit.class.isInstance(stat)) {
            if (xid == 0) {
                throw Error.NoTransactionException;
            }
            byte[] commit = tbm.commit(this.xid);
            this.xid = 0;
            return commit;
        } else if (Abort.class.isInstance(stat)) {
            if(xid == 0) {
                throw Error.NoTransactionException;
            }
            byte[] abort = tbm.abort(this.xid);
            xid = 0;
            return abort;
        } else {
            return execute2(stat);
        }
    }

    private byte[] execute2(Object stat) throws Exception {
        /*
            1.如果没指定事务，那么就分配一个临时事务。
            2.根据stat对象的类型调用响应方法。
         */
        boolean tempTransaction = false;
        Exception e = null;
        if (this.xid == 0) {
            tempTransaction = true;
            BeginRes begin = tbm.begin(new Begin());
            this.xid = begin.xid;
        }
        try {
            byte[] res = null;
            if(Show.class.isInstance(stat)) {
                res = tbm.show(xid);
            } else if(Create.class.isInstance(stat)) {
                res = tbm.create(xid, (Create)stat);
            } else if(Select.class.isInstance(stat)) {
                res = tbm.read(xid, (Select)stat);
            } else if(Insert.class.isInstance(stat)) {
                res = tbm.insert(xid, (Insert)stat);
            } else if(Delete.class.isInstance(stat)) {
                res = tbm.delete(xid, (Delete)stat);
            } else if(Update.class.isInstance(stat)) {
                res = tbm.update(xid, (Update)stat);
            }
            return res;
        } catch (Exception e1) {
            e = e1;
            throw e;
        } finally {
            if (tempTransaction) {
                if (e != null) {
                    tbm.abort(xid);
                } else {
                    tbm.commit(xid);
                }
                xid = 0;
            }
        }
    }

    /**
     * close一个执行器会回滚当前未提交的事务。
     */
    public void close() {
        if (xid != 0) {
            System.out.println("Abnormal Abort: " + xid);
            tbm.abort(xid);
        }
    }
}
