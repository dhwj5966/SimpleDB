package top.wuzonghui.simpledb.backend.tbm;

import com.google.common.primitives.Bytes;
import top.wuzonghui.simpledb.backend.im.BPlusTree;
import top.wuzonghui.simpledb.backend.parser.parser.statement.SingleExpression;
import top.wuzonghui.simpledb.backend.tm.TransactionManagerImpl;
import top.wuzonghui.simpledb.backend.utils.Panic;
import top.wuzonghui.simpledb.backend.utils.ParseStringRes;
import top.wuzonghui.simpledb.backend.utils.Parser;
import top.wuzonghui.simpledb.common.Error;

import java.util.Arrays;
import java.util.List;

/**
 * @author Starry
 * @create 2023-01-06-1:46 AM
 * @Describe 字段的抽象。一个Field对象就是一个表字段的抽象。由于 TBM 基于 VM，单个字段信息和表信息都是直接保存在 Entry 中。
 * 二进制格式为：
 * [FieldName][TypeName][IndexUid]
 * 如果field无索引，IndexUid为0。
 * 其中FieldName，TypeName这样的字符串，在数据库中存储的方式都是[StringLength][StringData]。
 */
public class Field {
    /**
     * 该字段的uid，也是实际存储的Entry的uid。
     */
    long uid;

    /**
     * 字段所属的表。
     */
    private Table tb;

    /**
     * 字段的名字。
     */
    String fieldName;

    /**
     * 字段的类型，目前只支持int32,int64,string
     */
    String fieldType;

    /**
     * 如果index=0，则说明该Field不存在索引，如果不为0，则代表该Field的索引对应的B+树的bootDataItem的uid。
     */
    private long index;

    /**
     * 当前字段(只有索引字段有)的索引，一颗B+树。
     */
    private BPlusTree bt;

    /**
     * @Describe 根据Field所属的Table和Field实际存储的Entry的uid，加载一个Field对象。
     * @param tb  该字段所属的Table对象。
     * @param uid Field实际存储的Entry的uid。
     * @return 加载出的Field对象。
     */
    public static Field loadField(Table tb, long uid) {
        byte[] raw = null;
        try {
            //[FieldName][TypeName][IndexUid]
            raw = ((TableManagerImpl) tb.tbm).vm.read(TransactionManagerImpl.SUPER_XID, uid);
        } catch (Exception e) {
            Panic.panic(e);
        }
        assert raw != null;
        return new Field(uid, tb).parseSelf(raw);
    }

    public Field(long uid, Table tb) {
        this.uid = uid;
        this.tb = tb;
    }

    public Field(Table tb, String fieldName, String fieldType, long index) {
        this.tb = tb;
        this.fieldName = fieldName;
        this.fieldType = fieldType;
        this.index = index;
    }

    /**
     * @Describe 通过解析按Field格式的byte数组，[FieldName][TypeName][IndexUid]，将FileName
     * @param raw
     * @return
     */
    private Field parseSelf(byte[] raw) {
        //1.解析fieldName
        int position = 0;
        ParseStringRes res = Parser.parseString(raw);
        fieldName = res.str;
        position += res.next;
        //2.解析fieldType
        res = Parser.parseString(Arrays.copyOfRange(raw, position, raw.length));
        fieldType = res.str;

        //解析出indexUid
        position += res.next;
        this.index = Parser.parseLong(Arrays.copyOfRange(raw, position, position + 8));
        //如果index不为0，说明该字段存在索引。
        if (index != 0) {
            try {
                bt = BPlusTree.load(index, ((TableManagerImpl) tb.tbm).dm);
            } catch (Exception e) {
                Panic.panic(e);
            }
        }
        return this;
    }

    /**
     * @Describe 创建一个Field对象，并通过VersionManager将Field的信息封装成[FieldName][TypeName][IndexUid]的标准格式，并插入到数据库中。
     * @param tb 该字段所属的Table。
     * @param xid 创建字段的事务的xid。
     * @param fieldName 该字段的名字。
     * @param fieldType 该字段的数据类型。
     * @param indexed true：是一个索引字段。false：不是一个索引字段。
     * @return 创建出的Field对象。
     * @throws Exception
     */
    public static Field createField(Table tb, long xid, String fieldName, String fieldType, boolean indexed) throws Exception {
        //检查fieldType
        typeCheck(fieldType);

        Field f = new Field(tb, fieldName, fieldType, 0);
        //如果是一个索引
        if (indexed) {
            //创建一颗B+树，拿到该B+树的bootDataItem的uid。
            long index = BPlusTree.create(((TableManagerImpl) tb.tbm).dm);
            //用B+树的bootDataItem的uid读取到B+树。
            BPlusTree bt = BPlusTree.load(index, ((TableManagerImpl) tb.tbm).dm);
            f.index = index;
            f.bt = bt;
        }
        //将该字段信息插入到数据库中
        f.persistSelf(xid);
        return f;
    }

    /**
     * @Describe 根据该Field的数据，生成符合格式的字节调用VersionManager的insert方法,将该Field的信息插入到数据库中，并将插入返回的uid赋给该Field的uid字段。
     * @param xid
     * @throws Exception
     */
    private void persistSelf(long xid) throws Exception {
        byte[] nameRaw = Parser.string2Byte(fieldName);
        byte[] typeRaw = Parser.string2Byte(fieldType);
        byte[] indexRaw = Parser.long2Byte(index);
        this.uid = ((TableManagerImpl) tb.tbm).vm.insert(xid, Bytes.concat(nameRaw, typeRaw, indexRaw));
    }

    /**
     * @Describe 类型检查。检查fieldType是否是int32,int64,string中的一个。
     * @param fieldType 该字段的数据类型。
     * @throws Exception 如果fieldType不是int32，int64，string中的一个，报错。
     */
    private static void typeCheck(String fieldType) throws Exception {
        if (!"int32".equals(fieldType) && !"int64".equals(fieldType) && !"string".equals(fieldType)) {
            throw Error.InvalidFieldException;
        }
    }

    /**
     * @Describe 该字段是否存在索引。
     * @return true：存在。false：不存在。
     */
    public boolean isIndexed() {
        return index != 0;
    }

    /**
     * @Describe 向该Field索引的B+树中，插入son-key对。
     * @param key key，可以是String，也可以是int，也可以是long。
     * @param uid son(uid)。数据实际存储的DataItem的uid。
     * @throws Exception
     */
    public void insert(Object key, long uid) throws Exception {
        long uKey = value2Uid(key);
        bt.insert(uKey, uid);
    }

    /**
     * @Describe 利用当前字段的B+树索引，搜索所有key在[left,right]中的数据。
     * @param left 左边界。
     * @param right 右边界
     * @return List  uids，存储了所有在范围里的数据的uid。
     * @throws Exception
     */
    public List<Long> search(long left, long right) throws Exception {
        return bt.searchRange(left, right);
    }

    /**
     * @Describe 根据当前Field的fieldType，解析字符串。
     * @param str 待解析的字符串。
     * 1.如果当前field的fieldType是int32，调用Integer.parseInt。
     * 2.如果当前field的fieldType是int64，调用Long.parseLong。
     * 3.如果当前field的fieldType是string，直接返回string。
     */
    public Object string2Value(String str) {
        switch (fieldType) {
            case "int32":
                return Integer.parseInt(str);
            case "int64":
                return Long.parseLong(str);
            case "string":
                return str;
        }
        return null;
    }

    /**
     * @Describe 计算Object key在B+树中的long key。
     * @param key
     * @return long类型的返回值。
     * @Instance
     */
    public long value2Uid(Object key) {
        long uid = 0;
        switch (fieldType) {
            case "string":
                uid = Parser.str2Uid((String) key);
                break;
            case "int32":
                int uint = (int) key;
                return (long) uint;
            case "int64":
                uid = (long) key;
                break;
        }
        return uid;
    }

    /**
     * @Describe 根据该Field的FieldType，将Object v解析成字节数组。
     * @param v 待解析的对象。
     * @return 解析后的字节数组。
     * @Detail 根据当前字段的数据类型进行解析：
     * 1.如果当前Field的FieldType是int32(int)，则v的类型也一定是int，将v强转成int，并解析成长度为4的byte数组。
     * 2.如果当前Field的FieldType是int64(long),则v的类型也一定是long，将v强转成long，并解析成长度为8的byte数组。
     * 3.如果当前Field的FieldType是string，则v的类型一定是string，为了将string转换为符合数据库存储的字符串格式。
     * 将v的长度作为前4位，与string的byte[]组成返回值并返回。
     * @Instance 比如该表字段的数据类型是int，Object为3，则返回字节数组[0,0,0,3]。
     */
    public byte[] value2Raw(Object v) {
        byte[] raw = null;
        switch (fieldType) {
            case "int32":
                raw = Parser.int2Byte((int) v);
                break;
            case "int64":
                raw = Parser.long2Byte((long) v);
                break;
            case "string":
                raw = Parser.string2Byte((String) v);
                break;
        }
        return raw;
    }

    class ParseValueRes {
        Object v;
        int shift;
    }

    public ParseValueRes parserValue(byte[] raw) {
        ParseValueRes res = new ParseValueRes();
        switch (fieldType) {
            case "int32":
                res.v = Parser.parseInt(Arrays.copyOf(raw, 4));
                res.shift = 4;
                break;
            case "int64":
                res.v = Parser.parseLong(Arrays.copyOf(raw, 8));
                res.shift = 8;
                break;
            case "string":
                ParseStringRes r = Parser.parseString(raw);
                res.v = r.str;
                res.shift = r.next;
                break;
        }
        return res;
    }

    /**
     * @Describe 将Object v转换成String类型。
     * @return 根据fieldType进行转换。
     */
    public String printValue(Object v) {
        String str = null;
        switch (fieldType) {
            case "int32":
                str = String.valueOf((int) v);
                break;
            case "int64":
                str = String.valueOf((long) v);
                break;
            case "string":
                str = (String) v;
                break;
        }
        return str;
    }

    @Override
    public String toString() {
        return new StringBuilder("(")
                .append(fieldName)
                .append(", ")
                .append(fieldType)
                .append(index != 0 ? ", Index" : ", NoIndex")
                .append(")")
                .toString();
    }

    /**
     * @Describe 解析SingleExpression对象，返回FieldCalRes对象。
     * @param exp
     * @return FieldCalRes对象，包括long left,long right两个字段。
     * @throws Exception
     * @Instance 比如exp.field = id, exp.compareOp = ">", exp.value = 7。
     * 则会根据索引，查询出id > 7的leftKey和rightKey，封装到FieldCalRes对象中返回。
     */
    public FieldCalRes calExp(SingleExpression exp) throws Exception {
        FieldCalRes result = new FieldCalRes();
        switch (exp.compareOp) {
            case ">" :
                //右边界是最大值没什么好说的。
                result.right = Long.MAX_VALUE;
                //将exp中的string value，解析成Object value
                Object value1 = string2Value(exp.value);
                //根据Object value，算出key。
                long l = value2Uid(value1) + 1;
                result.left = l;
                break;
            case "<" :
                result.left = 0;
                Object value2 = string2Value(exp.value);
                long l2 = value2Uid(value2);
                result.right = l2 > 0 ? l2 - 1 : l2;
                break;
            case "=" :
                long l3 = value2Uid(string2Value(exp.value));
                result.left = l3;
                result.right = l3;
                break;
        }
        return result;
    }
}
