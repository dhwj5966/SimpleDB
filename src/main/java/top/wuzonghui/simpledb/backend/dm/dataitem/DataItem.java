package top.wuzonghui.simpledb.backend.dm.dataitem;

import com.google.common.primitives.Bytes;
import top.wuzonghui.simpledb.backend.common.SubArray;
import top.wuzonghui.simpledb.backend.dm.DataManagerImpl;
import top.wuzonghui.simpledb.backend.dm.page.Page;
import top.wuzonghui.simpledb.backend.utils.Parser;

import java.util.Arrays;

/**
 * @author Starry
 * @create 2022-12-28-5:21 PM
 * @Describe 1.DataItem是DataManager层向上层提供的对数据的封装以及抽象。
 * 2.上层模块通过地址，向DataManager层请求到对应的DataItem，再获取其中的数据。
 * 3.上层模块对DataItem进行修改时，需要遵循一定的流程：在修改之前需要调用before()方法，想要撤销修改时调用unBefore()方法，修改完成后调用after()方法。
 * 4.这个流程的目的是保存前相数据，并及时落日志。DataManager会保证对DataItem的修改是原子性的。
 * @Detail DataItem中数据的存储格式：[ValidFlag,1byte],[DataSize,2byte],[Data]。
 * 其中ValidFlag标识了该DataItem是否处于被删除的状态;DataSize长度为2字节，标识了Data部分的长度。
 */
public interface DataItem {
    /**
     * 通过该方法可以获取该DataItem对象的Data部分。由于该方法返回的数组是数据共享的，而不是拷贝的，所有使用SubArray。
     */
    SubArray data();

    /**
     * 修改DataItem前，需要调用该方法。
     * 具体操作：
     * 1.开启写锁
     * 2.将DataItem所处的Page的dirty字段设置为true
     * 3.将原数据复制到临时数组，方便撤销操作。
     */
    void before();

    /**
     * 如果要撤销修改，则要调用该方法。
     * 具体操作：
     * 1.释放写锁
     * 2.将临时数组里的数据复制回原数据位置
     */
    void unBefore();

    /**
     * 修改完成后需要调用该方法。
     * 具体操作：
     * 1.记录日志
     * 2.释放写锁
     *
     * @param xid
     */
    void after(long xid);

    /**
     * 从内存中释放当前DataItem，注意：如果还有其他引用，则还会存在在缓存中。
     */
    void release();

    /**
     * 获取该DataItem的写锁。
     */
    void lock();

    /**
     * 释放该DataItem的写锁。
     */
    void unlock();

    /**
     * 获取该DataItem的读锁。
     */
    void rLock();

    /**
     * 释放该DataItem的读锁。
     */
    void rUnLock();

    /**
     * 获取该DataItem所在的Page对象。
     */
    Page page();

    /**
     * 获取该DataItem的uid(即DataItem的id)。
     *
     * @return
     */
    long getUid();

    /**
     * 获取OldRaw字段。oldRaw字段是一个临时数组，在修改数据的时候，将旧数据存储到临时数组里，以便撤销。
     *
     * @return
     */
    byte[] getOldRaw();

    /**
     * 获取Raw字段，Raw字段是DataItem的数据部分，
     * 包括[ValidFlag,1byte],[DataSize,2byte],[Data]。 封装成SubArray的目的是让多个DataItem可以操作同一个byte[]的不同部分。
     */
    SubArray getRaw();

    /**
     * 将byte[] raw的index为0的位设置为1，说明该DataItem数据已经被逻辑删除。
     *
     * @param raw
     */
    static void setDataItemRawInvalid(byte[] raw) {
        raw[DataItemImpl.OF_VALID] = (byte) 1;
    }

    /**
     * 根据int值pgno和short值offset，拼接成8字节的uid，其中前4个字节是pgno，后4个字节是offset
     *
     * @param pgno
     * @param offset
     * @return
     */
    static long addressToUid(int pgno, short offset) {
        long u0 = (long) pgno;
        long u1 = (long) offset;
        return u0 << 32 | u1;
    }

    /**
     * 工具方法，根据page，offset，dataManager，封装DataItem对象。
     *
     * @param page
     * @param offset
     * @param dataManager
     * @return
     */
    static DataItem parseDataItem(Page page, short offset, DataManagerImpl dataManager) {
        //raw是page的data部分
        byte[] raw = page.getData();
        //从raw的指定offset处解析出DataItem的Datasize
        short size = Parser.parseShort(Arrays.copyOfRange(raw, offset + DataItemImpl.OF_SIZE, offset + DataItemImpl.OF_DATA));
        //那么整个Datasize的长度就是Datasize + 3
        short length = (short) (size + DataItemImpl.OF_DATA);
        //根据page的pageno和传入的offset拼接出uid
        long uid = addressToUid(page.getPageNumber(), offset);
        return new DataItemImpl(
                new SubArray(raw, offset, offset + length),
                new byte[length],
                page,
                uid,
                dataManager);
    }

    /**
     * DataItem中数据的存储格式：[ValidFlag,1byte],[DataSize,2byte],[Data]。
     * 根据传入的DataItem的Data，封装一个DataItem格式的信息。
     * 比如传入[2,4,7,1],则返回[0,0,4,2,4,7,1]
     *
     * @param data
     * @return
     */
    static byte[] wrapDataItemRaw(byte[] data) {
        byte[] valid = new byte[1];
        byte[] dataSize = Parser.short2Byte((short) data.length);
        return Bytes.concat(valid, dataSize, data);
    }
}
