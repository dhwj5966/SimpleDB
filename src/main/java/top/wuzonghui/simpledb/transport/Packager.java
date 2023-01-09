package top.wuzonghui.simpledb.transport;

import java.io.IOException;

/**
 * @author Starry
 * @create 2023-01-08-1:55 PM
 * @Describe Transporter和Encoder类的组合类，对外提供服务。
 */
public class Packager {
    private Transporter transporter;
    private Encoder encoder;

    public Packager(Transporter transporter, Encoder encoder) {
        this.transporter = transporter;
        this.encoder = encoder;
    }

    public void close() throws IOException {
        transporter.close();
    }

    /**
     * 从Socket的输入流中读取数据，并封装成Package对象返回。
     */
    public Package receive() throws Exception {
        byte[] receive = transporter.receive();
        Package decode = encoder.decode(receive);
        return decode;
    }

    /**
     * 将Package中的数据编码成字节数组的形式，并通过Socket的输出流发送数据。
     */
    public void send(Package pkg) throws Exception {
        transporter.send(encoder.encode(pkg));
    }
}
