package top.wuzonghui.simpledb.backend.tbm;

import top.wuzonghui.simpledb.backend.utils.Panic;
import top.wuzonghui.simpledb.backend.utils.Parser;
import top.wuzonghui.simpledb.common.Error;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

/**
 * @author Starry
 * @create 2023-01-08-12:59 AM
 * @Describe 用一个文件，记录第一个表的UID。
 */
public class Booter {
    public static final String BOOTER_SUFFIX = ".bt";

    public static final String BOOTER_TMP_SUFFIX = ".bt_tmp";

    String path;
    File file;

    private Booter(String path, File file) {
        this.path = path;
        this.file = file;
    }

    public static Booter create(String path) {
        removeBadTmp(path);
        File file = new File(path + BOOTER_SUFFIX);
        try {
            if (!file.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        } catch (IOException e) {
            Panic.panic(e);
        }
        if (!file.canWrite() || !file.canRead()) {
            Panic.panic(Error.FileCannotRWException);
        }
        return new Booter(path, file);
    }

    public static Booter open(String path) {
        removeBadTmp(path);
        File f = new File(path + BOOTER_SUFFIX);
        if (!f.exists()) {
            Panic.panic(Error.FileNotExistsException);
        }
        if (!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }
        return new Booter(path, f);
    }

    /**
     * 将指定路径的.bt_tmp文件删除
     *
     * @param path
     */
    private static void removeBadTmp(String path) {
        new File(path + BOOTER_TMP_SUFFIX).delete();
    }

    /**
     * 读取.bt文件的所有数据，以byte[]的形式返回。
     *
     * @return 返回的是一个长度为8的字节数组，记录了第一个Table的uid。
     */
    public byte[] load() {
        byte[] bytes = new byte[0];
        try {
            bytes = Files.readAllBytes(file.toPath());
        } catch (IOException e) {
            Panic.panic(e);
        }
        return bytes;
    }

    /**
     * 将.bt文件的数据更新为data，确保更新的原子性。
     * @param data
     */
    public void update(byte[] data) {
        //1.先创建一个.bt_tmp结尾的文件。
        File tmp = new File(path + BOOTER_TMP_SUFFIX);
        try {
            tmp.createNewFile();
        } catch (Exception e) {
            Panic.panic(e);
        }
        if (!tmp.canRead() || !tmp.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }
        //2.将数据写到.bt_temp结尾的文件中。
        try (FileOutputStream out = new FileOutputStream(tmp)) {
            out.write(data);
            out.flush();
        } catch (IOException e) {
            Panic.panic(e);
        }
        //3.调用Files类的move方法，将.bt_tmp文件移动为.bt文件。
        try {
            Files.move(tmp.toPath(), new File(path + BOOTER_SUFFIX).toPath(), StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            Panic.panic(e);
        }
        file = new File(path + BOOTER_SUFFIX);
        if (!file.canRead() || !file.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }
    }
}
