package com.alibaba.datax.plugin.writer.pgcopywriter;

import java.io.IOException;
<<<<<<< HEAD
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.LinkedTransferQueue;
import java.util.stream.Collectors;

=======
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.stream.Collectors;

import com.alibaba.datax.common.element.Column;
>>>>>>> save
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.writer.CommonRdbmsWriter;
<<<<<<< HEAD
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
=======
>>>>>>> save
import org.postgresql.core.BaseConnection;

/**
 * <p>
<<<<<<< HEAD
 * CopyWriterTask
 * </p>
 *
 * @since 1.0
 */
public class CopyWriterTask extends CommonRdbmsWriter.Task {

    private volatile boolean stopWatcher = false;
=======
 *  CopyWriterTask
 * </p>
 * @since 1.0
 */
public class CopyWriterTask extends CommonRdbmsWriter.Task {
    private static final char FIELD_DELIMITER = '|';
    private static final char NEWLINE = '\n';
    private static final char QUOTE = '"';
    private static final char ESCAPE = '\\';
>>>>>>> save

    public CopyWriterTask() {
        super(DataBaseType.PostgreSQL);
    }

    @Override
    public void startWrite(RecordReceiver recordReceiver, Configuration writerSliceConfig, TaskPluginCollector taskPluginCollector) {
        Connection connection = DBUtil.getConnection(this.dataBaseType, this.jdbcUrl, username, password);
        // 配置为mysql方式处理
        DBUtil.dealWithSessionConfig(connection, writerSliceConfig, DataBaseType.MySql, BASIC_MESSAGE);
<<<<<<< HEAD
        this.resultSetMetaData = DBUtil.getColumnMetaData(connection, this.table, StringUtils.join(this.columns, ","));
        String headerSql = getCopySql(this.table, this.columns);
        LinkedTransferQueue<byte[]> dataQueue = new LinkedTransferQueue<>();
        CopyWorker copyWorker;
        Exception dataInError = null;
        try {
            copyWorker = new CopyWorker(writerSliceConfig, this, (BaseConnection) connection, headerSql, dataQueue);
            if (!writerSliceConfig.getBool("readerTest", false)) {
                copyWorker.start();
            }
=======
        String headerSql = getCopySql(this.table, this.columns);
        PipedOutputStream out = new PipedOutputStream();
        CopyWorker worker;
        try {
            worker = new CopyWorker((BaseConnection) connection, headerSql, new PipedInputStream(out));
            worker.start();
>>>>>>> save
        } catch (SQLException | IOException e) {
            throw DataXException.asDataXException(DBUtilErrorCode.WRITE_DATA_ERROR, e);
        }
        try {
            Record record;
            while ((record = recordReceiver.getFromReader()) != null) {
<<<<<<< HEAD
                // 写入copy时做转换，提升读取源端效率
                dataQueue.offer(CopyHelper.serializeRecord(record, this.columnNumber, this.getResultSetMetaData()));
            }
            this.stopWatcher = true;
        } catch (Exception e) {
            dataInError = e;
            try {
                ((BaseConnection) connection).cancelQuery();
            } catch (SQLException ignore) {
                // ignore
            }
            throw DataXException.asDataXException(DBUtilErrorCode.WRITE_DATA_ERROR, e);
        } finally {
            try {
                copyWorker.join();
=======
                if (record.getColumnNumber() != this.columnNumber) {
                    // 源头读取字段列数与目的表字段写入列数不相等，直接报错
                    throw DataXException.asDataXException(DBUtilErrorCode.CONF_ERROR,
                            String.format("列配置信息有错误. 因为您配置的任务中，源头读取字段数:%s 与 目的表要写入的字段数:%s 不相等. 请检查您的配置并作出修改.",
                                    record.getColumnNumber(), this.columnNumber));
                }
                byte[] data = serializeRecord(record);
                out.write(data);
            }
            out.flush();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                out.close();
            } catch (IOException e) {
                // ignore
            }
            try {
                worker.join();
>>>>>>> save
            } catch (InterruptedException e) {
                // ignore
            }
            DBUtil.closeDBResources(null, null, connection);
<<<<<<< HEAD
            if (dataInError == null) {
                if (copyWorker.getException() != null) {
                    throw DataXException.asDataXException(DBUtilErrorCode.WRITE_DATA_ERROR, copyWorker.getException());
                }
            }
        }

=======
        }
>>>>>>> save
    }

    private String getCopySql(String tableName, List<String> columns) {
        StringBuilder sb = new StringBuilder();
        sb.append("COPY ").append(tableName).append("(")
                .append(columns.stream().collect(Collectors.joining("\",\"", "\"", "\"")))
<<<<<<< HEAD
                .append(") FROM STDIN WITH DELIMITER '|' NULL '' CSV QUOTE '\"' ESCAPE E'\\\\'");
        return sb.toString();
    }

    public Triple<List<String>, List<Integer>, List<String>> getResultSetMetaData() {
        return this.resultSetMetaData;
    }

    public int getColumnNumber() {
        return this.columnNumber;
    }

    public boolean moreRecord() {
        return !stopWatcher;
=======
                .append(") FROM STDIN BINARY");
        return sb.toString();
    }

    private byte[] serializeRecord(Record record) throws UnsupportedEncodingException {
        StringBuilder sb = new StringBuilder();
        Column column;
        for (int i = 0; i < this.columnNumber; i++) {
            column = record.getColumn(i);
            int columnType = this.resultSetMetaData.getMiddle().get(i);
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
            if (i + 1 < this.columnNumber) {
                sb.append(FIELD_DELIMITER);
            }
        }
        // 换行符为 \n
        sb.append(NEWLINE);
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    protected String escapeString(String data) {
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

    /**
     * Non-printable characters are inserted as '\nnn' (octal) and '\' as '\\'.
     */
    private String escapeBinary(byte[] data) {
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
>>>>>>> save
    }

}
