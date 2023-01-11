package top.wuzonghui.simpledb.transport;

import com.google.common.primitives.Bytes;
import top.wuzonghui.simpledb.common.Error;

import java.util.Arrays;

/**
 * @author Starry
 * @create 2023-01-08-1:44 PM
 * @Describe 该类用来编码、解码。将字节数组解码成Package对象，将Pageage对象编码成字节数组。
 * 编码解码的规则如下：字节数组的格式为[flag][data]。
 * 如果flag为0，说明发送的是数据，
 * 如果flag为1则说明发送的是错误，data是Exception.getMessage()的错误提示信息getBytes()
 */
public class Encoder {
    public static final int FLAG_DATA = 0;
    public static final int FLAG_EXCEPTION = 1;


    /**
     * 将Package编码成字节数组。
     */
    public byte[] encode(Package pkg) {
        if(pkg.getErr() != null) {
            Exception err = pkg.getErr();
            String msg = "Intern server error!";
            if(err.getMessage() != null) {
                msg = err.getMessage();
            }
            return Bytes.concat(new byte[]{1}, msg.getBytes());
        } else {
            return Bytes.concat(new byte[]{0}, pkg.getData());
        }
    }

    /**
     * 解码，将data解析为Package对象。
     */
    public Package decode(byte[] data) throws Exception {
        if (data.length < 1) {
            throw Error.InvalidPkgDataException;
        }
        if (data[0] == 0) {
            return new Package(Arrays.copyOfRange(data, 1, data.length), null);
        } else if (data[0] == 1) {
            return new Package(null, new RuntimeException(new String(Arrays.copyOfRange(data, 1, data.length))));
        } else {
            throw Error.InvalidPkgDataException;
        }
    }
}
