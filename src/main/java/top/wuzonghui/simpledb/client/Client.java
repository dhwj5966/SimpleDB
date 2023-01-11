package top.wuzonghui.simpledb.client;


import top.wuzonghui.simpledb.transport.Package;
import top.wuzonghui.simpledb.transport.Packager;

/**
 * 客户端类
 */
public class Client {
    private RoundTripper rt;

    public Client(Packager packager) {
        this.rt = new RoundTripper(packager);
    }

    /**
     * 客户端执行。将数据byte[] stat包装成Package对象，并发送给服务器，并将服务器的返回结果返回。
     */
    public byte[] execute(byte[] stat) throws Exception {
        Package pkg = new Package(stat, null);
        Package aPackage = rt.roundTrip(pkg);
        if (aPackage.getErr() != null) {
            throw aPackage.getErr();
        }
        return aPackage.getData();
    }

    /**
     * 释放资源
     */
    public void close() {
        try {
            rt.close();
        } catch (Exception e) {

        }
    }

}
