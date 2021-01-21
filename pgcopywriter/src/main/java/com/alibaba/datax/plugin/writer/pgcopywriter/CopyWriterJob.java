package com.alibaba.datax.plugin.writer.pgcopywriter;

import java.util.List;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.writer.CommonRdbmsWriter;

/**
 * <p>
 * CopyWriterJob
 * </p>
 *
 * @author JupiterMouse
 * @since 1.0
 */
public class CopyWriterJob extends CommonRdbmsWriter.Job {
    public CopyWriterJob() {
        super(DataBaseType.PostgreSQL);
    }

    @Override
    public void init(Configuration originalConfig) {
        super.init(originalConfig);
    }

    @Override
    public void prepare(Configuration originalConfig) {
        super.prepare(originalConfig);
    }

    @Override
    public void post(Configuration originalConfig) {
        super.post(originalConfig);
    }

    @Override
    public List<Configuration> split(Configuration originalConfig, int mandatoryNumber) {
        return super.split(originalConfig, mandatoryNumber);
    }
}
