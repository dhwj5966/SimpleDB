package top.wuzonghui.simpledb.client;

import top.wuzonghui.simpledb.transport.Package;
import top.wuzonghui.simpledb.transport.Packager;

/**
 * 将Packager对象再封装，实现单次收发的逻辑。
 */
public class RoundTripper {
    private Packager packager;

    public RoundTripper(Packager packager) {
        this.packager = packager;
    }

    /**
     * 单次收发逻辑，调用Packager对象的send(Package pkg)方法发送数据，再调用receive()方法接收返回结果。
     */
    public Package roundTrip(Package pkg) throws Exception {
        packager.send(pkg);
        return packager.receive();
    }

    /**
     * 释放资源
     */
    public void close() throws Exception {
        packager.close();
    }
}
