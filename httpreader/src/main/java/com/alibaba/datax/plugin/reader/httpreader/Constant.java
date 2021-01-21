package com.alibaba.datax.plugin.reader.httpreader;

/**
 * <p>
 * 静态变量
 * </p>
 *
 * @author JupiterMouse 2020/8/5
 * @since 1.0
 */
public class Constant {

    public static final String CONNECT_TIMEOUT = "socket_timeout";

    public static final int DEFAULT_HTTP_TIMEOUT = 1000 * 60 * 60;

    public static final String POOL_SIZE = "pool_size";

    public static final int DEFAULT_POOL_SIZE = 5;

    public static final String DEFAULT_COLUMN_TYPE = "string";

    public static final String COLUMN_ENTRY_PATTERN = "[{";

    public static final String COLUMN_ARRAY_PATTERN = "[\"";

    public static final String HTTP_HEADER_CONTENT_TYPE = "Content-Type";
}
