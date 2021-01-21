package com.alibaba.datax.plugin.writer.pgcopywriter;

import java.util.List;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.writer.CommonRdbmsWriter;
import com.alibaba.datax.plugin.rdbms.writer.Key;

/**
 * <p>
 * <a href="https://www.postgresql.org/docs/9.6/sql-copy.html">sql-copy</a>
 * Pg Copy from STDIN
 * </p>
 *
 * @since 1.0
 */
public class CopyWriter extends Writer {

    public static class Job extends Writer.Job {
        private Configuration originalConfig = null;
        private CommonRdbmsWriter.Job delegate = null;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();

            // warn：not like mysql, PostgreSQL only support insert mode, don't use
            String writeMode = this.originalConfig.getString(Key.WRITE_MODE);
            if (null != writeMode) {
                throw DataXException.asDataXException(DBUtilErrorCode.CONF_ERROR,
                        String.format("写入模式(writeMode)配置有误. 因为PostgreSQL不支持配置参数项 writeMode: %s, PostgreSQL仅使用insert sql 插入数据. 请检查您的配置并作出修改.", writeMode));
            }
            delegate = new CopyWriterJob();
            delegate.init(this.originalConfig);
        }

        @Override
        public void prepare() {
            this.delegate.prepare(this.originalConfig);
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            return delegate.split(this.originalConfig, mandatoryNumber);
        }

        @Override
        public void post() {
            this.delegate.post(this.originalConfig);
        }

        @Override
        public void destroy() {
            this.delegate.destroy(this.originalConfig);
        }
    }

    public static class Task extends Writer.Task {
        private Configuration writerSliceConfig;
        private CommonRdbmsWriter.Task delegate;

        @Override
        public void init() {
            writerSliceConfig = super.getPluginJobConf();
            delegate = new CopyWriterTask();
            delegate.init(writerSliceConfig);
        }

        @Override
        public void prepare() {
            delegate.prepare(writerSliceConfig);
        }

        @Override
        public void post() {
            delegate.post(writerSliceConfig);
        }

        @Override
        public void destroy() {
            delegate.destroy(writerSliceConfig);
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            delegate.startWrite(lineReceiver, writerSliceConfig, super.getTaskPluginCollector());
        }
    }

}
