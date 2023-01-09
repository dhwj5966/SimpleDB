package top.wuzonghui.simpledb.backend.dm;

import com.google.common.primitives.Bytes;
import top.wuzonghui.simpledb.backend.common.SubArray;
import top.wuzonghui.simpledb.backend.dm.dataitem.DataItem;
import top.wuzonghui.simpledb.backend.dm.logger.Logger;
import top.wuzonghui.simpledb.backend.dm.page.Page;
import top.wuzonghui.simpledb.backend.dm.page.PageX;
import top.wuzonghui.simpledb.backend.dm.pagecache.PageCache;
import top.wuzonghui.simpledb.backend.tm.TransactionManager;
import top.wuzonghui.simpledb.backend.utils.Panic;
import top.wuzonghui.simpledb.backend.utils.Parser;

import java.util.*;

/**
 * @author Starry
 * @create 2022-12-27-6:07 PM
 * @Describe
 */
public class Recover {
    private static final byte LOG_TYPE_INSERT = 0;
    private static final byte LOG_TYPE_UPDATE = 1;

    private static final int REDO = 0;
    private static final int UNDO = 1;

    private static final int OF_TYPE = 0;
    private static final int OF_XID = OF_TYPE + 1;
    private static final int OF_UPDATE_UID = OF_XID + 8;
    private static final int OF_UPDATE_RAW = OF_UPDATE_UID + 8;
    private static final int OF_INSERT_PGNO = OF_XID + 8;
    private static final int OF_INSERT_OFFSET = OF_INSERT_PGNO + 4;
    private static final int OF_INSERT_RAW = OF_INSERT_OFFSET + 2;


    //insertLog格式 [LogType,1byte] [XID,8byte] [Pgno,4byte] [Offset,2byte] [Raw]
    static class InsertLogInfo {
        long xid;
        int pgno;
        short offset;
        byte[] raw;
    }

    //UpdateLog格式 [LogType,1byte] [XID,8byte] [UID,8byte] [OldRaw] [NewRaw]
    static class UpdateLogInfo {
        long xid;
        int pgno;
        short offset;
        byte[] oldRaw;
        byte[] newRaw;
    }

    /**
     * 生成一个updateLog。格式：[LogType,1byte] [XID,8byte] [UID,8byte] [OldRaw] [NewRaw]。
     * 其中LogType = 1， uid，oldRaw，newRaw都从di中获取。
     * @param xid
     * @param di
     * @return
     */
    public static byte[] updateLog(long xid, DataItem di) {
        byte[] logType = {LOG_TYPE_UPDATE};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] uidRaw = Parser.long2Byte(di.getUid());
        byte[] oldRaw = di.getOldRaw();
        SubArray raw = di.getRaw();
        byte[] newRaw = Arrays.copyOfRange(raw.raw, raw.start, raw.end);
        return Bytes.concat(logType, xidRaw, uidRaw, oldRaw, newRaw);
    }

    /**
     * 生成一个insertLog。格式：[LogType,1byte] [XID,8byte] [Pgno,4byte] [Offset,2byte] [Raw]
     * @param xid 执行insert操作的事务的xid
     * @param pg 要插入到的page
     * @param raw 要插入的具体数据
     * @return
     */
    public static byte[] insertLog(long xid, Page pg, byte[] raw) {
        byte[] logType = new byte[1];
        logType[0] = LOG_TYPE_INSERT;
        byte[] xidBytes = Parser.long2Byte(xid);
        byte[] pageNoBytes = Parser.int2Byte(pg.getPageNumber());
        byte[] offsetBytes = Parser.short2Byte(PageX.getFSO(pg));
        return Bytes.concat(logType, xidBytes, pageNoBytes, offsetBytes, raw);
    }

    /**
     * 根据日志进行数据恢复。遍历所有log，找到log操作的最大数据页的页号，根据最大页号截断.db文件。
     * 遍历日志文件，redo所有状态为commited或aborted的事务。
     * 遍历日志文件，undo所有状态为active的事务。
     * @param tm
     * @param lg
     * @param pc
     */
    public static void recover(TransactionManager tm, Logger lg, PageCache pc) {
        System.out.println("Recovering...");
        //从头开始遍历日志
        lg.rewind();
        //记录所有日志操作的pageno中，最大的pageno
        int maxPgno = 0;
        while(true) {
            //获取一条日志的数据
            byte[] log = lg.next();
            if(log == null) break;
            int pgno;
            if(isInsertLog(log)) {
            //如果是一条insertLog
                InsertLogInfo li = parseInsertLog(log);
                pgno = li.pgno;
            } else {
                //如果是一条updateLog
                UpdateLogInfo li = parseUpdateLog(log);
                pgno = li.pgno;
            }
            if(pgno > maxPgno) {
                maxPgno = pgno;
            }
        }
        if(maxPgno == 0) {
            maxPgno = 1;
        }
        pc.truncateByPgno(maxPgno);
        System.out.println("Truncate to " + maxPgno + " pages.");


        redoTranscations(tm, lg, pc);
        System.out.println("Redo Transactions Over.");

        undoTranscations(tm, lg, pc);
        System.out.println("Undo Transactions Over.");

        System.out.println("Recovery Over.");
    }




    /**
     * 通过lg对象，遍历日志文件，undo所有状态为active的事务。
     * @param tm
     * @param lg
     * @param pc
     */
    private static void undoTranscations(TransactionManager tm, Logger lg, PageCache pc) {
        //以事务为单位，对所有active的事务的操作，倒叙进行undo。
        //map中存储的是{xid -> [log1, log2,... logn]}
        Map<Long, List<byte[]>> map = new HashMap<>();
        lg.rewind();
        while (true) {
            byte[] log = lg.next();
            if (log == null) break;
            if (isInsertLog(log)) {
            //如果是insert log。
                InsertLogInfo insertLogInfo = parseInsertLog(log);
                long xid = insertLogInfo.xid;
                if (tm.isActive(xid)) {
                    if (map.containsKey(xid)) {
                        map.get(xid).add(log);
                    } else {
                        List<byte[]> list = new ArrayList<>();
                        list.add(log);
                        map.put(xid, list);
                    }
                }
            } else {
                //如果是update log
                UpdateLogInfo updateLogInfo = parseUpdateLog(log);
                long xid = updateLogInfo.xid;
                if (tm.isActive(xid)) {
                    if (map.containsKey(xid)) {
                        map.get(xid).add(log);
                    } else {
                        List<byte[]> list = new ArrayList<>();
                        list.add(log);
                        map.put(xid, list);
                    }
                }
            }
        }

        //遍历map，对每个事务的log，倒叙undo
        for (Map.Entry<Long, List<byte[]>> entry : map.entrySet()) {
            List<byte[]> logs = entry.getValue();
            for (int i = logs.size() - 1; i >= 0; i--) {
                byte[] log = logs.get(i);
                if (isInsertLog(log)) {
                    doInsertLog(pc, log, UNDO);
                } else {
                    doUpdateLog(pc, log, UNDO);
                }
            }
            //将事务状态设为已回滚
            tm.abort(entry.getKey());
        }

    }


    /**
     * @param tm TransactionManager对象
     * @param lg Logger对象
     * @param pc PageCache对象
     * @Describe 通过lg对象，遍历日志文件，redo所有状态为commited或aborted的事务。
     */
    private static void redoTranscations(TransactionManager tm, Logger lg, PageCache pc) {
        /*
            从头开始遍历日志信息，循环获取每一条日志。
            获取一条log的data部分，如果log的data部分为空，证明已经遍历结束，break。
            如果是一条insertlog，则将logdata封装成insertLogInfo，如果该条log对应的事务已经提交或者回滚，则redo。
            如果是一条updatelog，则将logdata封装成updateLogInfo，如果该条log对应的事务已经提交或者回滚，则redo。
         */
        lg.rewind();
        while (true) {
            //获取一条log的data部分
            byte[] log = lg.next();
            //如果日志为空，证明已经遍历结束，break
            if (log == null) break;
            //如果这是一条Insert的log(通过logtype判断)
            if (isInsertLog(log)) {
                //将日志携带的信息解析成InsertLogInfo对象。
                InsertLogInfo li = parseInsertLog(log);
                //xid是产生此条日志的事务的id
                long xid = li.xid;
                //如果该事务已经提交或者回滚，则redo。
                if (!tm.isActive(xid)) {
                    doInsertLog(pc, log, REDO);
                }
            } else {
                //如果这是一条update的log(通过logtype判断)
                UpdateLogInfo xi = parseUpdateLog(log);
                long xid = xi.xid;
                if (!tm.isActive(xid)) {
                    doUpdateLog(pc, log, REDO);
                }
            }
        }
    }


    /**
     * 将字节数组解析成UpdateLogInfo对象。
     *
     * @param log
     * @return
     */
    private static UpdateLogInfo parseUpdateLog(byte[] log) {
        //UpdateLog格式 [LogType,1byte] [XID,8byte] [UID,8byte] [OldRaw] [NewRaw]
        UpdateLogInfo li = new UpdateLogInfo();
        li.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_UPDATE_UID));
        long uid = Parser.parseLong(Arrays.copyOfRange(log, OF_UPDATE_UID, OF_UPDATE_RAW));
        //uid占位8字节，其中48-64字节是offset，0-32字节是pgno。
        short offset = (short) (uid & ((1L << 16) - 1));
        li.offset = offset;
        uid >>>= 32;
        int pgno = (int) (uid & ((1L << 32) - 1));
        li.pgno = pgno;
        //将剩下的长度一分为二，一半是oldRaw，一半是newRaw
        int length = (log.length - OF_UPDATE_RAW) / 2;
        li.oldRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW, OF_UPDATE_RAW + length);
        li.newRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW + length, OF_UPDATE_RAW + length * 2);
        return li;
    }

    /**
     * 根据传入的log，redo或者undo update操作
     *
     * @param pc
     * @param log
     * @param redo
     */
    private static void doUpdateLog(PageCache pc, byte[] log, int redo) {
        /*
            redo update:重新修改一遍数据
            undo update:将数据从old raw改为 new raw
            提示：可以调用PageX.recoverUpdate()
         */
        UpdateLogInfo updateLogInfo = parseUpdateLog(log);
        Page page = null;
        try {
            page = pc.getPage(updateLogInfo.pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        byte[] raw = null;
        //如果是要redo update
        if (redo == REDO) {
            //把新数据放到指定位置
            raw = updateLogInfo.newRaw;
        } else {
            //如果是undo update，把旧数据放到指定位置
            raw = updateLogInfo.oldRaw;
        }
        try {
            PageX.recoverUpdate(page, raw, updateLogInfo.offset);
        } finally {
            //释放page
            page.release();
        }
    }

    /**
     * 根据传入的log，redo或者undo insert操作。
     * 是redo还是undo区别于参数flag。
     * @param pc
     * @param log
     * @param flag if flag = Recover.UNDO，执行undo操作。否则执行redo操作。
     */
    private static void doInsertLog(PageCache pc, byte[] log, int flag) {
        /*
            首先分析：insertLog的redo和undo分别要做什么？
            insertLog的格式是[LogType,1byte] [XID,8byte] [Pgno,4byte] [Offset,2byte] [Raw]
            如果是要redo：需要把数据重新insert一遍，那么重新insert一遍数据。
            如果是要undo：需要把该行数据删除掉，将数据设置为逻辑删除，然后重新insert进去，但是insert进去的是逻辑删除的数据。
         */
        InsertLogInfo insertLogInfo = parseInsertLog(log);
        Page page = null;
        try {
            page = pc.getPage(insertLogInfo.pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        byte[] raw = insertLogInfo.raw;
        try {
            if (flag == UNDO) {
                DataItem.setDataItemRawInvalid(raw);
            }
            PageX.recoverInsert(page, raw, insertLogInfo.offset);
        } finally {
            page.release();
        }


    }


    /**
     * 根据byte数组的第一位，判断当前byte数组代表insert log还是update log
     * @param log
     * @return
     */
    private static boolean isInsertLog(byte[] log) {
        return log[0] == LOG_TYPE_INSERT;
    }

    /**
     * 将字节数组解析成insertlogInfo对象
     *
     * @param log
     * @return
     */
    private static InsertLogInfo parseInsertLog(byte[] log) {
        ////insertLog格式 [LogType,1byte] [XID,8byte] [Pgno,4byte] [Offset,2byte] [Raw]
        InsertLogInfo insertLogInfo = new InsertLogInfo();
        insertLogInfo.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_INSERT_PGNO));
        insertLogInfo.pgno = Parser.parseInt(Arrays.copyOfRange(log, OF_INSERT_PGNO, OF_INSERT_OFFSET));
        insertLogInfo.offset = Parser.parseShort(Arrays.copyOfRange(log, OF_INSERT_OFFSET, OF_INSERT_RAW));
        insertLogInfo.raw = Arrays.copyOfRange(log, OF_INSERT_RAW, log.length);
        return insertLogInfo;
    }


}

