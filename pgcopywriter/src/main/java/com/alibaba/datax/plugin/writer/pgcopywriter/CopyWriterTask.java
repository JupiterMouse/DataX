package com.alibaba.datax.plugin.writer.pgcopywriter;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.LinkedTransferQueue;
import java.util.stream.Collectors;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.writer.CommonRdbmsWriter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.postgresql.core.BaseConnection;

/**
 * <p>
 * CopyWriterTask
 * </p>
 *
 * @since 1.0
 */
public class CopyWriterTask extends CommonRdbmsWriter.Task {

    private volatile boolean stopWatcher = false;

    public CopyWriterTask() {
        super(DataBaseType.PostgreSQL);
    }

    @Override
    public void startWrite(RecordReceiver recordReceiver, Configuration writerSliceConfig, TaskPluginCollector taskPluginCollector) {
        Connection connection = DBUtil.getConnection(this.dataBaseType, this.jdbcUrl, username, password);
        // 配置为mysql方式处理
        DBUtil.dealWithSessionConfig(connection, writerSliceConfig, DataBaseType.MySql, BASIC_MESSAGE);
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
        } catch (SQLException | IOException e) {
            throw DataXException.asDataXException(DBUtilErrorCode.WRITE_DATA_ERROR, e);
        }
        try {
            Record record;
            while ((record = recordReceiver.getFromReader()) != null) {
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
            } catch (InterruptedException e) {
                // ignore
            }
            DBUtil.closeDBResources(null, null, connection);
            if (dataInError == null) {
                if (copyWorker.getException() != null) {
                    throw DataXException.asDataXException(DBUtilErrorCode.WRITE_DATA_ERROR, copyWorker.getException());
                }
            }
        }

    }

    private String getCopySql(String tableName, List<String> columns) {
        StringBuilder sb = new StringBuilder();
        sb.append("COPY ").append(tableName).append("(")
                .append(columns.stream().collect(Collectors.joining("\",\"", "\"", "\"")))
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
    }

}
