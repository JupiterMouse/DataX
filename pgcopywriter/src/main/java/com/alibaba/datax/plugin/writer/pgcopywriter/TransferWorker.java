package com.alibaba.datax.plugin.writer.pgcopywriter;

import java.util.List;
import java.util.concurrent.LinkedTransferQueue;

import com.alibaba.datax.common.element.Record;
import org.apache.commons.lang3.tuple.Triple;

/**
 * <p>
 * 转换
 * </p>
 *
 * @author JupiterMouse
 * @since 1.0
 */
public class TransferWorker extends Thread {

    private final CopyWriterTask task;
    private final LinkedTransferQueue<Record> transferQueue;
    private final LinkedTransferQueue<byte[]> dataQueue;
    private final int columnNumber;
    private final Triple<List<String>, List<Integer>, List<String>> resultSetMetaData;

    public TransferWorker(CopyWriterTask task,
                          LinkedTransferQueue<Record> transferQueue,
                          LinkedTransferQueue<byte[]> dataQueue) {
        this.task = task;
        this.transferQueue = transferQueue;
        this.dataQueue = dataQueue;
        this.columnNumber = task.getColumnNumber();
        this.resultSetMetaData = task.getResultSetMetaData();
    }

    @Override
    public void run() {
        Record record;
        while (true) {
            record = transferQueue.poll();
            if (record != null) {
                dataQueue.offer(CopyHelper.serializeRecord(record, columnNumber, resultSetMetaData));
            }
            if (record == null && !this.task.moreRecord()) {
                break;
            }
        }
        task.setTransferWatcher(true);
    }
}
