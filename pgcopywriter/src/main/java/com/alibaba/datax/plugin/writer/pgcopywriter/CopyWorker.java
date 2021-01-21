package com.alibaba.datax.plugin.writer.pgcopywriter;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.LinkedTransferQueue;

import com.alibaba.datax.common.util.Configuration;
import org.postgresql.PGConnection;
import org.postgresql.copy.PGCopyOutputStream;
import org.postgresql.core.BaseConnection;


/**
 * <p>
 * <a href="https://support.huaweicloud.com/devg-dws/dws_04_0853.html"></a>
 * </p>
 *
 * @since 1.0
 */
public class CopyWorker extends Thread {

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
    }

    @Override
    public void run() {
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        byte[] record;
        PGCopyOutputStream os = null;
        try {
            os = new PGCopyOutputStream(conn, this.headerSql);
            int index = 0;
            while (true) {
                try {
                    record = dateQueue.poll();
                } catch (Exception e) {
                    record = null;
                    e.printStackTrace();
                }
                if (record == null && !this.task.moreRecord()) {
                    break;
                }
                if (record != null) {
                    os.write(record);
                    ++index;
                }
                if (index == copySize) {
                    os.close();
                    os = new PGCopyOutputStream(conn, this.headerSql);
                    index = 0;
                }
            }
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
}
