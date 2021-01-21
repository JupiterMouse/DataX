package com.alibaba.datax.plugin.writer.pgcopywriter;

import java.io.IOException;
<<<<<<< HEAD
<<<<<<< HEAD
import java.sql.SQLException;
import java.util.concurrent.LinkedTransferQueue;

import com.alibaba.datax.common.util.Configuration;
import org.postgresql.PGConnection;
import org.postgresql.copy.PGCopyOutputStream;
=======
import java.io.InputStream;
=======
>>>>>>> v1
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.LinkedTransferQueue;

<<<<<<< HEAD
import org.postgresql.copy.CopyManager;
>>>>>>> save
=======
import com.alibaba.datax.common.util.Configuration;
import org.apache.commons.lang3.tuple.Triple;
import org.postgresql.PGConnection;
import org.postgresql.copy.PGCopyOutputStream;
>>>>>>> v1
import org.postgresql.core.BaseConnection;


/**
 * <p>
 * <a href="https://support.huaweicloud.com/devg-dws/dws_04_0853.html"></a>
 * </p>
 *
 * @since 1.0
 */
public class CopyWorker extends Thread {
<<<<<<< HEAD
<<<<<<< HEAD

    private final CopyWriterTask task;
    private final PGConnection conn;
    private final String headerSql;
    private final LinkedTransferQueue<byte[]> dateQueue;
    private final Integer copySize;
    private final static String COPY_SIZE = "copySize";
    private Exception exception = null;

    public CopyWorker(Configuration conf, CopyWriterTask task, BaseConnection conn, String headerSql,
                      LinkedTransferQueue<byte[]> dateQueue) throws SQLException, IOException {
        this.task = task;
        this.conn = conn;
        this.headerSql = headerSql;
        this.dateQueue = dateQueue;
        this.setName(this.headerSql);
        // 适度大，可以提升效率
        this.copySize = conf.getInt(COPY_SIZE, 102400);
=======
    private final CopyManager copyManager;
=======

    private final CopyWriterTask task;
    private final PGConnection conn;
>>>>>>> v1
    private final String headerSql;
    private final LinkedTransferQueue<byte[]> dateQueue;
    private final Integer copySize;
    private final static String COPY_SIZE = "copySize";
    private final int columnNumber;
    private final Triple<List<String>, List<Integer>, List<String>> resultSetMetaData;
    private Exception exception = null;

    public CopyWorker(Configuration conf, CopyWriterTask task, BaseConnection conn, String headerSql,
                      LinkedTransferQueue<byte[]> dateQueue) throws SQLException, IOException {
        this.task = task;
        this.conn = conn;
        this.headerSql = headerSql;
        this.dateQueue = dateQueue;
        this.columnNumber = task.getColumnNumber();
        this.resultSetMetaData = task.getResultSetMetaData();
        this.setName(this.headerSql);
<<<<<<< HEAD
>>>>>>> save
=======
        // 越大越好
        this.copySize = conf.getInt(COPY_SIZE, 102400);
>>>>>>> v1
    }

    @Override
    public void run() {
        byte[] record;
        PGCopyOutputStream os = null;
        try {
<<<<<<< HEAD
<<<<<<< HEAD
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        byte[] record;
        PGCopyOutputStream os = null;
        try {
=======
>>>>>>> v1
            os = new PGCopyOutputStream(conn, this.headerSql);
            int index = 0;
            while (true) {
                try {
                    record = dateQueue.poll();
                } catch (Exception e) {
                    record = null;
                    e.printStackTrace();
                }
<<<<<<< HEAD
                if (record == null && !this.task.moreRecord()) {
=======
                if (record == null && !this.task.moreTransform()) {
>>>>>>> v1
                    break;
                }
                if (record != null) {
                    os.write(record);
<<<<<<< HEAD
=======
//                    os.write(CopyHelper.serializeRecord(record, columnNumber, resultSetMetaData));
>>>>>>> v1
                    ++index;
                }
                if (index == copySize) {
                    os.close();
                    os = new PGCopyOutputStream(conn, this.headerSql);
                    index = 0;
                }
            }
<<<<<<< HEAD
        } catch (SQLException | IOException e) {
            exception = e;
        } finally {
            if (os != null) {
                try {
                    os.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public Exception getException() {
        return exception;
    }
=======
            copyManager.copyIn(headerSql, is);
=======
>>>>>>> v1
        } catch (SQLException | IOException e) {
            exception = e;
        } finally {
            if (os != null) {
                try {
                    os.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
<<<<<<< HEAD
>>>>>>> save
=======

    public Exception getException() {
        return exception;
    }
>>>>>>> v1
}
