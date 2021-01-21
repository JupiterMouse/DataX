package com.alibaba.datax.plugin.writer.pgcopywriter;

<<<<<<< HEAD
=======
import java.io.UnsupportedEncodingException;
>>>>>>> v1
import java.nio.charset.StandardCharsets;
import java.sql.Types;
import java.util.List;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * CopyHelper
 * </p>
 *
 * @author JupiterMouse
 * @since 1.0
 */
public class CopyHelper {
    private static final Logger LOG = LoggerFactory.getLogger(CopyHelper.class);
    private static final char FIELD_DELIMITER = '|';
    private static final char NEWLINE = '\n';
    private static final char QUOTE = '"';
    private static final char ESCAPE = '\\';

    public static String escapeString(String data) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < data.length(); ++i) {
            char c = data.charAt(i);
            switch (c) {
                case 0x00:
                    LOG.warn("字符串中发现非法字符 0x00，已经将其删除");
                    continue;
                case QUOTE:
                case ESCAPE:
                    sb.append(ESCAPE);
                default:
                    // nothing
            }
            sb.append(c);
        }
        return sb.toString();
    }


    public static byte[] serializeRecord(Record record, int columnNumber, Triple<List<String>, List<Integer>,
<<<<<<< HEAD
            List<String>> resultSetMetaData) {
=======
            List<String>> resultSetMetaData){
>>>>>>> v1
        StringBuilder sb = new StringBuilder();
        Column column;
        for (int i = 0; i < columnNumber; i++) {
            column = record.getColumn(i);
            int columnType = resultSetMetaData.getMiddle().get(i);
            // 类型处理
            switch (columnType) {
                // 字符类型
                case Types.CHAR:
                case Types.NCHAR:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.NVARCHAR:
                case Types.LONGNVARCHAR: {
                    String data = column.asString();
                    if (data != null) {
                        sb.append(QUOTE);
                        // 去除00x00字符
                        sb.append(escapeString(data));
                        sb.append(QUOTE);
                    }

                    break;
                }
                // bin 类型
                case Types.BINARY:
                case Types.BLOB:
                case Types.CLOB:
                case Types.LONGVARBINARY:
                case Types.NCLOB:
                case Types.VARBINARY: {
                    byte[] data = column.asBytes();
                    if (data != null) {
                        // \nnn \ -> \\
                        sb.append(escapeBinary(data));
                    }
                    break;
                }
                // 其它走string
                default: {
                    String data = column.asString();
                    if (data != null) {
                        sb.append(data);
                    }
                    break;
                }
            }
            // 分隔符为 |
            if (i + 1 < columnNumber) {
                sb.append(FIELD_DELIMITER);
            }
        }
        // 换行符为 \n
        sb.append(NEWLINE);
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static String escapeBinary(byte[] data) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < data.length; ++i) {
            if (data[i] == '\\') {
                sb.append('\\');
                sb.append('\\');
            } else if (data[i] < 0x20 || data[i] > 0x7e) {
                byte b = data[i];
                char[] val = new char[3];
                val[2] = (char) ((b & 07) + '0');
                b >>= 3;
                val[1] = (char) ((b & 07) + '0');
                b >>= 3;
                val[0] = (char) ((b & 03) + '0');
                sb.append('\\');
                sb.append(val);
            } else {
                sb.append((char) (data[i]));
            }
        }

        return sb.toString();
    }
}
