package top.wuzonghui.simpledb.backend.dm.pageindex;

/**
 * @author Starry
 * @create 2022-12-28-1:17 AM
 * @Describe Bean对象，其field有int pgno,int freespace。
 */
public class PageInfo {
    /**
     * 页号
     */
    public int pgno;
    /**
     * 空闲空间
     */
    public int freespace;

    public PageInfo(int pgno, int freespace) {
        this.pgno = pgno;
        this.freespace = freespace;
    }
}
