package top.wuzonghui.simpledb.backend.utils;

public class ParseStringRes {
    /**
     * String
     */
    public String str;

    /**
     * 下一条String的起始位置。
     */
    public int next;

    public ParseStringRes(String str, int next) {
        this.str = str;
        this.next = next;
    }
}
