package top.wuzonghui.simpledb.client;



import top.wuzonghui.simpledb.transport.Encoder;
import top.wuzonghui.simpledb.transport.Packager;
import top.wuzonghui.simpledb.transport.Transporter;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

public class Launcher {
    public static void main(String[] args) throws UnknownHostException, IOException {
        Socket socket = new Socket("127.0.0.1", 9999);
        Transporter transporter = new Transporter(socket);
        Packager packager = new Packager(transporter, new Encoder());
        Client client = new Client(packager);
        Shell shell = new Shell(client);
        shell.run();
    }
}
