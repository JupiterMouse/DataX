package com.alibaba.datax.plugin.reader.httpreader;

/**
 * <p>
 * 配置文件字段信息
 * </p>
 *
 * @author JupiterMouse 2020/8/5
 * @since 1.0
 */
public final class Key {
    /**
     * 请求的URL
     */
    public final static String URL = "url";
    /**
     * 请求头
     */
    public final static String HEADERS = "headers";
    /**
     * 请求格式
     */
    public final static String FORMAT = "format";
    /**
     * 请求方法
     */
    public final static String METHOD = "method";
    /**
     * 请求数据
     */
    public final static String PAYLOAD = "payload";
    /**
     * 数据路径
     */
    public final static String MAP_PATH = "mapPath";
    /**
     * connect
     */
    public final static String MAP_CONNECTOR = "connect";
    /**
     * 认证标志
     */
    public final static String AUTH = "auth";
    /**
     * 认证标志
     */
    public final static String AUTH_TYPE = "type";
    /**
     * 用户名
     */
    public final static String USERNAME = "username";
    /**
     * 密码
     */
    public final static String PASSWORD = "password";


    /**
     * HTTP获取数据的列
     */
    public static final String HTTP_COLUMN = "column";
    /**
     * 每个列的名字
     */
    public static final String COLUMN_NAME = "name";
    /**
     * 每个列的类型
     */
    public static final String COLUMN_TYPE = "type";


}
