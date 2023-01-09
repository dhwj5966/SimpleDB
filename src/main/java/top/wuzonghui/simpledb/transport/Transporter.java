package top.wuzonghui.simpledb.transport;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import java.io.*;
import java.net.Socket;

/**
 * @author Starry
 * @create 2023-01-08-1:54 PM
 * @Describe 通过Socket和流，发送接受数据。
 */
public class Transporter {
    private Socket socket;
    private BufferedReader reader;
    private BufferedWriter writer;

    public Transporter(Socket socket) throws IOException {
        this.socket = socket;
        reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
    }

    /**
     * 发送数据
     * @param data
     */
    public void send(byte[] data) throws Exception {
        String raw = hexEncode(data);
        writer.write(raw);
        writer.flush();
    }

    /**
     * 接收数据
     *
     */
    public byte[] receive() throws Exception {
        String line = reader.readLine();
        if(line == null) {
            close();
        }
        return hexDecode(line);
    }

    /**
     * 释放资源，包括Socket和流。
     */
    public void close() throws IOException {
        writer.close();
        reader.close();
        socket.close();
    }

    /**
     * 将buffer的数据转换成16进制的String类型,加上\n。
     * @Instance
     * 1.buffer = [97,97,97], return 616161n
     * 2.buffer = [96,96,96], return 606060n
     * 3.buffer = [98,98,98],return 626262n
     */
    private String hexEncode(byte[] buffer) {
        return Hex.encodeHexString(buffer, true) + "\n";
    }

    /**
     * 16进制解码。
     * @Instance "616161" -> [97,97,97]
     */
    private byte[] hexDecode(String buffer) throws DecoderException {
        return Hex.decodeHex(buffer);
    }

}
