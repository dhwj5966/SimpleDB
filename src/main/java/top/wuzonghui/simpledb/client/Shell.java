package top.wuzonghui.simpledb.client;

import java.util.Scanner;

public class Shell {

    private Client client;

    public Shell(Client client) {
        this.client = client;
    }

    /**
     * 一个简易的Shell逻辑。
     */
    public void run() {
        //用Scanner不断地接收数据
        Scanner scanner = new Scanner(System.in);
        try {
            while (true) {
                System.out.print("simpleDB>");
                String s = scanner.nextLine();
                if("exit".equals(s) || "quit".equals(s)) {
                    break;
                }
                try {
                    byte[] result = client.execute(s.getBytes());
                    System.out.println(new String(result));
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                }
            }
        } finally {
            scanner.close();
            client.close();
        }
    }
}
