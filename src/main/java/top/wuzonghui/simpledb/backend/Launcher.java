package top.wuzonghui.simpledb.backend;

import org.apache.commons.cli.*;
import top.wuzonghui.simpledb.backend.dm.DataManager;
import top.wuzonghui.simpledb.backend.server.Server;
import top.wuzonghui.simpledb.backend.tbm.TableManager;
import top.wuzonghui.simpledb.backend.tm.TransactionManager;
import top.wuzonghui.simpledb.backend.utils.Panic;
import top.wuzonghui.simpledb.backend.vm.VersionManager;
import top.wuzonghui.simpledb.backend.vm.VersionManagerImpl;
import top.wuzonghui.simpledb.common.Error;

/**
 * @author Starry
 * @create 2023-01-08-7:27 PM
 * @Describe
 */
public class Launcher {

    //服务器端口号
    public static final int port = 9999;

    //默认内存大小
    public static final long DEFALUT_MEM = (1 << 20) * 64;
    public static final long KB = 1 << 10;
    public static final long MB = 1 << 20;
    public static final long GB = 1 << 30;

    public static void main(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption("open", true, "-open DBPath");
        options.addOption("create", true, "-create DBPath");
        options.addOption("mem", true, "-mem 64MB");
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        //如果命令里包含open参数，则以打开的方式，打开一个数据库。
        if (cmd.hasOption("open")) {
            openDB(cmd.getOptionValue("open"), parseMem(cmd.getOptionValue("mem")));
            return;
        }
        //如果命令里包含create参数，则以创建的方式，创建一个数据库。
        if (cmd.hasOption("create")) {
            createDB(cmd.getOptionValue("create"));
            return;
        }
        System.out.println("Usage: launcher (open|create) DBPath");
    }

    private static void createDB(String path) {
        TransactionManager tm = TransactionManager.create(path);
        DataManager dm = DataManager.create(path, DEFALUT_MEM, tm);
        VersionManager vm = VersionManager.newVersionManager(tm, dm);
        TableManager.create(path, vm, dm);

        tm.close();
        dm.close();
    }

    private static void openDB(String path, long mem) {
        TransactionManager tm = TransactionManager.open(path);
        DataManager dm = DataManager.open(path, mem, tm);
        VersionManager vm = new VersionManagerImpl(tm, dm);
        TableManager tbm = TableManager.open(path, vm, dm);
        Server server = new Server(port, tbm);
        server.start();
    }

    private static long parseMem(String memStr) {
        if(memStr == null || "".equals(memStr)) {
            return DEFALUT_MEM;
        }
        if(memStr.length() < 2) {
            Panic.panic(Error.InvalidMemException);
        }
        String unit = memStr.substring(memStr.length() - 2);
        long memNum = Long.parseLong(memStr.substring(0, memStr.length()-2));
        switch (unit) {
            case "KB" :
                return memNum * KB;
            case "MB" :
                return memNum * MB;
            case "GB" :
                return memNum * GB;
            default:
                Panic.panic(Error.InvalidMemException);
        }
        return DEFALUT_MEM;
    }
}
