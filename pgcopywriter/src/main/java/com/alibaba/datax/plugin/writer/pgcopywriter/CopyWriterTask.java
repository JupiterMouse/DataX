package com.alibaba.datax.plugin.writer.pgcopywriter;

import java.io.IOException;
<<<<<<< HEAD
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
=======
>>>>>>> v1
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.LinkedTransferQueue;
import java.util.stream.Collectors;

<<<<<<< HEAD
import com.alibaba.datax.common.element.Column;
>>>>>>> save
=======
>>>>>>> v1
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
<<<<<<< HEAD
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
=======
>>>>>>> save
=======
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
>>>>>>> v1
import org.postgresql.core.BaseConnection;

/**
 * <p>
<<<<<<< HEAD
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
=======
 * CopyWriterTask
>>>>>>> v1
 * </p>
 *
 * @since 1.0
 */
public class CopyWriterTask extends CommonRdbmsWriter.Task {

    private volatile boolean stopWatcher = false;
    private volatile boolean transferWatcher = false;
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
=======
        this.resultSetMetaData = DBUtil.getColumnMetaData(connection, this.table, StringUtils.join(this.columns, ","));
>>>>>>> v1
        String headerSql = getCopySql(this.table, this.columns);
        LinkedTransferQueue<Record> transferQueue = new LinkedTransferQueue<>();
        LinkedTransferQueue<byte[]> dataQueue = new LinkedTransferQueue<>();
        TransferWorker transferWorker;
        CopyWorker copyWorker;
        Exception dataInError = null;
        try {
<<<<<<< HEAD
            worker = new CopyWorker((BaseConnection) connection, headerSql, new PipedInputStream(out));
            worker.start();
>>>>>>> save
=======
            transferWorker = new TransferWorker(this, transferQueue, dataQueue);
            copyWorker = new CopyWorker(writerSliceConfig, this, (BaseConnection) connection, headerSql, dataQueue);
            if (!writerSliceConfig.getBool("readerTest", false)) {
                transferWorker.start();
                copyWorker.start();
            }
>>>>>>> v1
        } catch (SQLException | IOException e) {
            throw DataXException.asDataXException(DBUtilErrorCode.WRITE_DATA_ERROR, e);
        }
        try {
            Record record;
            while ((record = recordReceiver.getFromReader()) != null) {
<<<<<<< HEAD
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
=======
                // 写入copy时做转换，提升读取源端效率
                transferQueue.offer(record);
>>>>>>> v1
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
<<<<<<< HEAD
                worker.join();
>>>>>>> save
=======
                copyWorker.join();
>>>>>>> v1
            } catch (InterruptedException e) {
                // ignore
            }
            DBUtil.closeDBResources(null, null, connection);
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> v1
            if (dataInError == null) {
                if (copyWorker.getException() != null) {
                    throw DataXException.asDataXException(DBUtilErrorCode.WRITE_DATA_ERROR, copyWorker.getException());
                }
            }
<<<<<<< HEAD
        }

=======
        }
>>>>>>> save
=======
        }

>>>>>>> v1
    }

    private String getCopySql(String tableName, List<String> columns) {
        StringBuilder sb = new StringBuilder();
        sb.append("COPY ").append(tableName).append("(")
                .append(columns.stream().collect(Collectors.joining("\",\"", "\"", "\"")))
<<<<<<< HEAD
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
=======
                .append(") FROM STDIN WITH DELIMITER '|' NULL '' CSV QUOTE '\"' ESCAPE E'\\\\'");
>>>>>>> v1
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
    }

<<<<<<< HEAD
        return sb.toString();
>>>>>>> save
=======
    public boolean moreTransform() {
        return !transferWatcher;
>>>>>>> v1
    }

    public void setTransferWatcher(boolean transferWatcher) {
        this.transferWatcher = transferWatcher;
    }
}
