package top.wuzonghui.simpledb.backend.server;

import top.wuzonghui.simpledb.backend.tbm.TableManager;
import top.wuzonghui.simpledb.transport.Encoder;
import top.wuzonghui.simpledb.transport.Package;
import top.wuzonghui.simpledb.transport.Packager;
import top.wuzonghui.simpledb.transport.Transporter;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author Starry
 * @create 2023-01-08-2:28 PM
 * @Describe 启动一个ServerSocket监听指定端口，当有请求到来的时候直接把请求丢给一个新线程处理。
 */
public class Server {

    /**
     * 监听的端口号
     */
    private int port;

    /**
     * tbm对象
     */
    TableManager tbm;

    public Server(int port, TableManager tbm) {
        this.port = port;
        this.tbm = tbm;
    }

    /**
     * 运行服务器，监控指定端口，当建立一条连接后，把Socket交给新线程处理。
     */
    public void start() {
        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(port);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        System.out.println("Server is running! Server listen to port: " + port);
        ThreadPoolExecutor threadPool = new ThreadPoolExecutor(
                10,
                20,
                1L,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(100), new ThreadPoolExecutor.CallerRunsPolicy()
        );
        try {
            while (true) {
                Socket socket = serverSocket.accept();
                Runnable runnable = new HandleSocket(socket, tbm);
                threadPool.execute(runnable);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                serverSocket.close();
            } catch (IOException e) {

            }
        }
    }

}

class HandleSocket implements Runnable {
    private Socket socket;
    private TableManager tableManager;

    public HandleSocket(Socket socket, TableManager tableManager) {
        this.socket = socket;
        this.tableManager = tableManager;
    }

    /*
        当后端服务器接收到一个新连接时的处理逻辑。
        1.获取到新连接的客户端ip地址
        2.初始化Packager对象
        3.新建一个执行器对象。循环地接收来自客户端的数据。
        4.客户端断开连接后就close执行器和packager对象。
     */
    @Override
    public void run() {
        InetSocketAddress address = (InetSocketAddress) socket.getRemoteSocketAddress();
        System.out.println("Establish connection: " + address.getAddress().getHostAddress() + ":" + address.getPort());
        Packager packager = null;

        try {
            Transporter transporter = new Transporter(socket);
            Encoder encoder = new Encoder();
            packager = new Packager(transporter, encoder);
        } catch (IOException e) {
            e.printStackTrace();
            try {
                socket.close();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
            return;
        }

        Executor executor = new Executor(tableManager);

        while (true) {
            //接收客户端发来的信息
            Package pkg = null;
            try {
                pkg = packager.receive();
            } catch (Exception e) {
                break;
            }

            byte[] sql = pkg.getData();
            byte[] result = null;
            Exception e = null;

            //交给executor执行sql语句。
            try {
                result = executor.execute(sql);
            } catch (Exception ex) {
                e = ex;
                e.printStackTrace();
            }

            //将executor的返回结果发送给客户端。
            pkg = new Package(result, e);
            try {
                packager.send(pkg);
            } catch (Exception e1) {
                e1.printStackTrace();
                break;
            }
        }

        //
        executor.close();
        try {
            packager.close();
        } catch (Exception e2) {
            e2.printStackTrace();
        }
    }
}
