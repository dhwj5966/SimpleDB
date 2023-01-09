package top.wuzonghui.simpledb.transport;

/**
 * @author Starry
 * @create 2023-01-08-1:42 PM
 * @Describe 每个Package在发送前，由Encoder编码为字节数组，在接受到字节数组后也由Encoder解码成Package对象。
 *
 */
public class Package {
    byte[] data;
    Exception err;

    public Package(byte[] data, Exception err) {
        this.data = data;
        this.err = err;
    }

    public byte[] getData() {
        return data;
    }

    public Exception getErr() {
        return err;
    }
}
