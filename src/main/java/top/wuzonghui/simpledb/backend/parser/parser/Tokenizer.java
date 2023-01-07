package top.wuzonghui.simpledb.backend.parser.parser;

import top.wuzonghui.simpledb.common.Error;

/**
 * @author Starry
 * @create 2023-01-05-1:06 PM
 * @Describe 对外提供peek()和pop()方法，以此将语句按照
 */
public class Tokenizer {
    public static void main(String[] args) throws Exception {
        Tokenizer tokenizer = new Tokenizer("sele * from asd2_asd".getBytes());
        String peek = null;
        while (!(peek = tokenizer.peek()).equals("")) {
            System.out.println(peek);
            tokenizer.pop();
        }
    }
    private byte[] stat;

    //position
    private int pos;

    //当前的token
    private String currentToken;

    //是否需要刷新Token,当调用peek方法的时候，只有flushToken为true才会继续往下读取一个token。
    private boolean flushToken;
    private Exception err;

    public Tokenizer(byte[] stat) {
        this.stat = stat;
        this.pos = 0;
        this.currentToken = "";
        this.flushToken = true;
    }

    /**
     * 查看token，查看后如果要查看下一个token需要调用pop方法。
     * @return 如果已经到命令的末尾则返回 "" ;
     * @throws Exception
     */
    public String peek() throws Exception {
        //如果err字段不等于null，抛出异常。
        if (err != null) {
            throw err;
        }
        //如果需要刷新，更新currentToken。
        if (flushToken) {
            String token = null;
            try {
                //读取下一个token
                token = next();
            } catch (Exception e) {
                err = e;
                throw e;
            }
            currentToken = token;
            //不再需要刷新
            flushToken = false;
        }
        return currentToken;
    }

    private String next() throws Exception {
        if (err != null) {
            throw err;
        }
        return nextMetaState();
    }

    private String nextMetaState() throws Exception {
        while (true) {
            Byte b = peekByte();
            if (b == null) return "";
            if (!isBlank(b)) {
                break;
            }
            pos++;
        }
        //如果是符号
        Byte b = peekByte();
        if (isSymbol(b)) {
            pos++;
            return new String(new byte[]{b});
        } else if (b == '"' || b == '\'') {
            //如果b是"或者\ 那么直接往下个引号找。
            return nextQuoteState();
        } else if (isAlphaBeta(b) || isDigit(b)) {
            //b如果是字母或者数字，直接找到下一个Token
            return nextTokenState();
        } else {
            err = Error.InvalidCommandException;
            throw err;
        }
    }

    private String nextTokenState() {
        StringBuilder sb = new StringBuilder();
        while (true) {
            Byte b = peekByte();
            if (b == null || !(isAlphaBeta(b) || isDigit(b) || b == '_')) {
                if (b != null && isBlank(b)) {
                    pos++;
                }
                return sb.toString();
            }
            sb.append(new String(new byte[]{b}));
            pos++;
        }
    }

    static boolean isDigit(byte b) {
        return (b >= '0' && b <= '9');
    }

    static boolean isAlphaBeta(byte b) {
        return ((b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z'));
    }

    //找到下一个引号，将2个引号之间的数据作为字符串返回。
    private String nextQuoteState() throws Exception {
        byte quote = peekByte();
        pos++;
        StringBuilder sb = new StringBuilder();
        while (true) {
            Byte b = peekByte();
            if (b == null) {
                err = Error.InvalidCommandException;
                throw err;
            }
            if (b == quote) {
                pos++;
                break;
            }
            sb.append(new String(new byte[]{b}));
            pos++;
        }
        return sb.toString();
    }

    private boolean isSymbol(Byte b) {
        return (b == '>' || b == '<' || b == '=' || b == '*' ||
                b == ',' || b == '(' || b == ')');
    }

    private boolean isBlank(Byte b) {
        return (b == '\n' || b == ' ' || b == '\t');
    }


    private Byte peekByte() {
        if (pos >= stat.length) {
            return null;
        } else {
            return stat[pos];
        }
    }

    //
    public byte[] errStat() {
        byte[] res = new byte[stat.length + 3];
        System.arraycopy(stat, 0, res, 0, pos);
        System.arraycopy("<< ".getBytes(), 0, res, pos, 3);
        System.arraycopy(stat, pos, res, pos + 3, stat.length - pos);
        return res;
    }

    public void pop() {
        flushToken = true;
    }
}
