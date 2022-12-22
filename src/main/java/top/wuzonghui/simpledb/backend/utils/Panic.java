package top.wuzonghui.simpledb.backend.utils;

/**
 * @author Starry
 * @create 2022-12-22-7:13 PM
 * @Describe
 */
public class Panic {

    public static void panic(Exception exception) {
        exception.printStackTrace();
        System.exit(1);
    }
}
