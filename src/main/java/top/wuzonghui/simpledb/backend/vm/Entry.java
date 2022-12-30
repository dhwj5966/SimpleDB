package top.wuzonghui.simpledb.backend.vm;

/**
 * @author Starry
 * @create 2022-12-30-7:13 PM
 * @Describe 通过DM模块可以操作DataItem，VM则管理所有的DataItem，向上层提供Entry。
 * 上层通过VM模块操作数据的最小单位就是Entry。
 * 每个记录有多个版本(Version),当上层模块对Entry进行修改时，VM就会为这个Entry创建一个新的Version。
 * @Detail 格式：[XMIN, 8byte] [XMAX, 8byte] [data]
 */
public class Entry {
    /**
     * Entry中XMIN部分的offset
     */
    private static final int OF_XMIN = 0;

    /**
     * Entry中XMAX部分的offset
     */
    private static final int OF_XMAX = OF_XMIN+8;

    /**
     * Entry中data部分的offset
     */
    private static final int OF_DATA = OF_XMAX+8;
}
