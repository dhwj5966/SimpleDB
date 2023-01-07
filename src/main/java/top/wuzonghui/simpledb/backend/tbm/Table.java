package top.wuzonghui.simpledb.backend.tbm;

import com.google.common.primitives.Bytes;
import top.wuzonghui.simpledb.backend.parser.parser.statement.*;
import top.wuzonghui.simpledb.backend.tm.TransactionManagerImpl;
import top.wuzonghui.simpledb.backend.utils.Panic;
import top.wuzonghui.simpledb.backend.utils.ParseStringRes;
import top.wuzonghui.simpledb.backend.utils.Parser;
import top.wuzonghui.simpledb.backend.vm.Transaction;
import top.wuzonghui.simpledb.backend.vm.VersionManager;
import top.wuzonghui.simpledb.common.Error;

import java.util.*;

/**
 * @author Starry
 * @create 2023-01-06-1:54 AM
 * @Describe
 * Table 维护了表结构
 * 二进制结构如下：
 * [TableName][NextTable]
 * [Field1Uid][Field2Uid]...[FieldNUid]
 */
public class Table {
    /**
     * 管理该Table的TableManager对象。
     */
    TableManager tbm;

    /**
     * 该表的uid，表存储在Entry中，该uid也是Entry的uid。
     */
    long uid;

    /**
     * 表名
     */
    String name;
    byte status;

    /**
     * 该表的下一张表的uid。
     */
    long nextUid;

    /**
     * 该表的字段。
     */
    List<Field> fields = new ArrayList<>();

    public Table(TableManager tbm, long uid) {
        this.tbm = tbm;
        this.uid = uid;
    }

    public Table(TableManager tbm, String tableName, long nextUid) {
        this.tbm = tbm;
        this.name = tableName;
        this.nextUid = nextUid;
    }

    /**
     * @Describe 通过tbm对象，和uid对象，加载Table对象。
     * @return 加载出的Table对象。
     * @Detail 会根据uid，利用vm读出该表的二进制数据，解析二进制数据生成Table对象。
     */
    public static Table loadTable(TableManager tbm, long uid) {
        TableManagerImpl tableManager = (TableManagerImpl) tbm;
        VersionManager vm = ((TableManagerImpl) tbm).vm;

        byte[] raw = null;
        try {
            raw = vm.read(TransactionManagerImpl.SUPER_XID, uid);
        } catch (Exception e) {
            Panic.panic(e);
        }

        Table table = new Table(tbm, uid);
        //根据table的二进制数据，解析出table对象。
        return table.parseSelf(raw);
    }

    /**
     * 创建一个新的Table对象，会将该表的二进制数据持久化到数据库中。
     * @param tbm 管理该表的TableManager对象。
     * @param nextUid 该Table的下一张表的uid。
     * @param xid 创建Table的事务的xid。
     * @param create 根据create语句封装出的create对象。
     * @return 创建的表对象。
     * @throws Exception
     */
    public static Table createTable(TableManager tbm, long nextUid, long xid, Create create) throws Exception {
        TableManagerImpl tableManagerImpl = (TableManagerImpl) tbm;
        VersionManager vm = ((TableManagerImpl) tbm).vm;
        Table table = new Table(tbm, create.tableName, nextUid);
        for (int i = 0; i < create.fieldName.length; i++) {
            String fieldName = create.fieldName[i];
            String fieldType = create.fieldType[i];
            boolean indexed = false;
            for (String b : create.index) {
                if (b.equals(fieldName)) {
                    indexed = true;
                    break;
                }
            }
            Field field = Field.createField(table, xid, fieldName, fieldType, indexed);
            table.fields.add(field);
        }

        //还要将该表作为一个Entry，插入到数据库中。
        return table.persistSelf(xid);
    }


    /**
     * 根据Table的二进制数据，解析出该表的字段值，并赋值给字段。在解析前，该Table已经有了uid和tbm。
     * [TableName][NextTable]
     * [Field1Uid][Field2Uid]...[FieldNUid]
     * @param raw 该表的二进制数据。
     * @return 将二进制数据解析并赋值给该表的字段后，将该表返回。
     */
    private Table parseSelf(byte[] raw) {
        int position = 0;
        ParseStringRes parseStringRes = Parser.parseString(raw);
        this.name = parseStringRes.str;

        position += parseStringRes.next;
        this.nextUid = Parser.parseLong(Arrays.copyOfRange(raw, position, position + 8));

        position += 8;

        while (position < raw.length) {
            long fieldUid = Parser.parseLong(Arrays.copyOfRange(raw, position, 8));
            Field field = Field.loadField(this, fieldUid);
            this.fields.add(field);
            position += 8;
        }
        return this;
    }

    //该表调用vm，根据自身信息，将自己持久化到数据库中。
    private Table persistSelf(long xid) throws Exception {
        //封装自身信息为raw
        byte[] tableNameRaw = Parser.string2Byte(this.name);
        byte[] nextTableRaw = Parser.long2Byte(this.nextUid);
        byte[] fieldUids = new byte[0];
        for (Field field : this.fields) {
            fieldUids = Bytes.concat(fieldUids, Parser.long2Byte(field.uid));
        }

        //根据自身持有的tbm，找到vm，insert
        TableManagerImpl tableManager = (TableManagerImpl) this.tbm;
        VersionManager vm = tableManager.vm;
        long insertUid = vm.insert(xid, Bytes.concat(tableNameRaw, nextTableRaw, fieldUids));
        this.uid = insertUid;
        return this;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder("{");
        sb.append(name).append(": ");
        for(Field field : fields) {
            sb.append(field.toString());
            if(field == fields.get(fields.size()-1)) {
                sb.append("}");
            } else {
                sb.append(", ");
            }
        }
        return sb.toString();
    }

    /**
     * @Describe 在该表上删除数据。
     * @param xid 执行删除操作的事务的xid。
     * @param delete 由delete语句封装出的Delete对象，包含了删除数据所需的所有信息。
     * @return 删除的行数。
     * @throws Exception
     */
    public int delete(long xid, Delete delete) throws Exception {
        //通过解析where语句，返回所有要删除的数据行的uid。
        List<Long> uids = parseWhere(delete.where);
        int count = 0;
        VersionManager versionManager = ((TableManagerImpl)this.tbm).vm;
        for (Long uid : uids) {
            if (versionManager.delete(xid, uid)) {
                count++;
            }
        }
        return count;
    }

    /**
     * @Describe update该表的数据。
     * @param xid 执行update操作的事务的xid。
     * @param update Update对象，封装了update所需的全部信息。
     * @return 更新的行数
     * @throws Exception
     */
    public int update(long xid, Update update) throws Exception {
        //那些需要更新数据的行数据的uid。
        List<Long> uids = parseWhere(update.where);
        //找到需要更新的Field。
        Field field = null;
        for (Field field1 : fields) {
            if (field1.fieldName.equals(update.fieldName)) {
                field = field1;
                break;
            }
        }
        if(field == null) {
            throw Error.FieldNotFoundException;
        }
        //将update对象中String类型的value对象，根据该field的数据类型，解析为对应的数据。
        Object value = field.string2Value(update.value);
        int count = 0;
        VersionManager versionManager = ((TableManagerImpl)this.tbm).vm;
        for (Long uid : uids) {
            /*
                更新数据实际上是将原来的行数据删除，再将新的数据插入到表中，记得更新索引。
             */
            //1.读出行数据
            byte[] raw = versionManager.read(xid, uid);
            if (raw == null) continue;
            //2.将行数据解析成Map
            Map<String, Object> map = parseEntry(raw);
            map.put(field.fieldName, value);
            //3.将map反解析为raw数据
            byte[] newRaw = entry2Raw(map);
            //4.将原数据删除，新数据插入
            versionManager.delete(xid, uid);
            long newUid = versionManager.insert(xid, newRaw);

            count++;

            //遍历Field，更新索引
            for (Field field1 : fields) {
                if (field1.isIndexed()) {
                    field1.insert(map.get(field.fieldName), newUid);
                }
            }
        }
        return count;
    }

    /**
     * @Describe 从该表中read数据。
     * @param xid read数据的事务的xid。
     * @param read Select对象，封装了读取数据所需的全部信息。
     * @return String。
     * @throws Exception
     */
    public String read(long xid, Select read) throws Exception {
        //找到那些满足where条件的数据行的uid。
        List<Long> uids = parseWhere(read.where);
        StringBuilder sb = new StringBuilder();
        for (Long uid : uids) {
            //读出数据行
            byte[] raw = ((TableManagerImpl)tbm).vm.read(xid, uid);
            if (raw == null) continue;
            Map<String, Object> entry = parseEntry(raw);
            sb.append(printEntry(entry)).append("\n");
        }
        return sb.toString();
    }

    /**
     * @Describe 在该表上执行insert操作。
     * @param xid 执行insert操作的事务的xid。
     * @param insert 封装好的insert对象，携带所有insert信息。
     * @throws Exception
     * @Detail 要插入的数据将会被封装成一行，并插入到数据库中。如果一个表字段存在索引，则更新索引。
     */
    public void insert(long xid, Insert insert) throws Exception {
        //根据insert的String[]数组，解析出map
        Map<String, Object> entry = string2Entry(insert.values);
        //根据map解析出一行数据的二进制格式。
        byte[] raw = entry2Raw(entry);
        //调用vm的insert方法，将这一行数据插入到数据库中，返回插入的记录的uid。
        long uid = ((TableManagerImpl)tbm).vm.insert(xid, raw);
        //遍历该表的所有表字段，如果该表字段存在索引，那么需要对索引进行更新。
        for (Field field : fields) {
            if (field.isIndexed()) {
                field.insert(entry.get(field.fieldName), uid);
            }
        }
    }

    /**
     * @Describe 根据该Table的字段，将values解析并且和表字段的name组成key-value。
     * @param values String数组，要插入的值。
     * @return Map。
     * @throws Exception
     * @Instance 比如该表的字段为id，name。values为["3","wzh"],则返回{"id" = 3, "name" = "wzh"}
     */
    private Map<String, Object> string2Entry(String[] values) throws Exception {
        if (values.length != fields.size()) {
            throw Error.InvalidValuesException;
        }
        Map<String, Object> map = new HashMap<>();
        for (int i = 0; i < values.length; i++) {
            Field field = fields.get(i);
            Object o = field.string2Value(values[i]);
            map.put(field.fieldName, o);
        }
        return map;
    }


    private String printEntry(Map<String, Object> entry) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            String value = field.printValue(entry.get(field.fieldName));
            sb.append(value);
            if (i == fields.size() - 1) {
                sb.append("]");
            } else {
                sb.append("\t");
            }
        }
        return sb.toString();
    }

    /**
     * @Describe 根据该Table的Field，将一行数据解析为"fieldName = fieldValue"对。
     * @param raw 待解析的数据行。
     * @return Map
     * @throws Exception
     */
    private Map<String, Object> parseEntry(byte[] raw) throws Exception{
        if (raw == null) {
            throw Error.InvalidFieldException;
        }
        int position = 0;
        Map<String, Object> map = new HashMap<>();
        for (Field field : this.fields) {
            Field.ParseValueRes parseValueRes = field.parserValue(Arrays.copyOfRange(raw, position, raw.length));
            map.put(field.fieldName, parseValueRes.v);
            position += parseValueRes.shift;
        }
        return map;
    }

    /**
     * @Describe 根据该表的表字段，将entry的数据解析成字节数组。(可以理解为一行数据)
     * @param entry Map对象，key为该表表字段的名字，value为数据行的值。
     * @return byte数组。
     * @Instance 该表的表字段为id int64, name string, age int32。
     * entry = {"id" = 4, "name" = "wzh", "age" = 24}。
     * 则返回值为[0,0,0,0,0,0,0,4] + [0,0,0,3] + "wzh".getBytes() + [0,0,0,24]。
     */
    private byte[] entry2Raw(Map<String, Object> entry) {
        byte[] raw = new byte[0];
        for (Field field : fields) {
            Object value = entry.get(field.fieldName);
            raw = Bytes.concat(raw, field.value2Raw(value));
        }
        return raw;
    }


    /**
     * @Describe 通过解析Where对象，返回所有Where对象包含的Entry的uid。
     * @param where 待解析的where对象。
     * @return 将在范围内的uid放到列表中返回。
     * @throws Exception
     * @Detail 关于where的规则。
     * 1.where只能作用于索引字段。
     * 2.where语句最多只有2个部分。
     * 3.where语句的fieldname必须一样。
     */
    private List<Long> parseWhere(Where where) throws Exception {
        /*
            如何解析Where语句？
         */
        long l0 = 0, r0 = 0, l1 = 0, r1 = 0;
        //如果where为null，则说明全表扫描
        Field whereField = null;
        boolean single = true;
        if (where == null) {

        } else {
            //where不为null，由于本数据库暂时只支持对单个字段的where语句，因此需要找到该字段。
            String fieldName = where.singleExp1.field;
            for (Field field : fields) {
                if (field.fieldName.equals(fieldName)) {
                    whereField = field;
                    break;
                }
            }
            if (whereField == null) {
                throw Error.FieldNotFoundException;
            }
            CalWhereRes calWhereRes = calWhere(whereField, where);
            l0 = calWhereRes.l0;
            r0 = calWhereRes.r0;
            l1 = calWhereRes.l1;
            r1 = calWhereRes.r1;
            single = calWhereRes.single;
        }
        List<Long> uids = whereField.search(l0, r0);
        if (!single) {
            uids.addAll(whereField.search(l1, r1));
        }
        return uids;
    }

    class CalWhereRes {
        long l0, r0, l1, r1;

        //true：where语句只有一个限制条件，或者两个限制条件用and连接。 false：两个限制条件，or连接。
        boolean single;
    }

    private CalWhereRes calWhere(Field field, Where where) throws Exception {
        CalWhereRes calWhereRes = new CalWhereRes();
        switch (where.logicOp) {
            case "":
                calWhereRes.single = true;
                FieldCalRes fieldCalRes = field.calExp(where.singleExp1);
                calWhereRes.l0 = fieldCalRes.left;
                calWhereRes.r0 = fieldCalRes.right;
                break;
            case "and":
                calWhereRes.single = true;
                FieldCalRes fieldCalRes1 = field.calExp(where.singleExp1);
                FieldCalRes fieldCalRes2 = field.calExp(where.singleExp2);
                calWhereRes.l0 = Math.max(fieldCalRes1.left, fieldCalRes2.left);
                calWhereRes.r0 = Math.min(fieldCalRes1.right, fieldCalRes2.right);
                break;
            case "or":
                calWhereRes.single = false;
                FieldCalRes fieldCalRes3 = field.calExp(where.singleExp1);
                FieldCalRes fieldCalRes4 = field.calExp(where.singleExp2);
                calWhereRes.l0 = fieldCalRes3.left;
                calWhereRes.r0 = fieldCalRes3.right;
                calWhereRes.l1 = fieldCalRes4.left;
                calWhereRes.r1 = fieldCalRes4.right;
                break;
            default:
                throw Error.InvalidLogOpException;
        }
        return calWhereRes;
    }

}
