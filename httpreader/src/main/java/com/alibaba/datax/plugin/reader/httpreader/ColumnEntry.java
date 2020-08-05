package com.alibaba.datax.plugin.reader.httpreader;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

import org.apache.commons.lang3.StringUtils;

/**
 * <p>
 * 列
 * </p>
 *
 * @author JupiterMouse 2020/8/5
 * @since 1.0
 */
public class ColumnEntry {
    private String type;
    private String name;
    private String format;
    private DateFormat dateParse;

    public ColumnEntry() {
    }

    public ColumnEntry(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
        if (StringUtils.isNotBlank(this.format)) {
            this.dateParse = new SimpleDateFormat(this.format);
        }
    }

    public DateFormat getDateFormat() {
        return this.dateParse;
    }

}
