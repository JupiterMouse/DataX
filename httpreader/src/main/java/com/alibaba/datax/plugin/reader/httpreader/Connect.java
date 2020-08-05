package com.alibaba.datax.plugin.reader.httpreader;

/**
 * <p>
 * HTTP 连接timeout
 * </p>
 *
 * @author JupiterMouse 2020/8/5
 * @since 1.0
 */
public class Connect {

    private int connectionRequestTimeout = -1;
    private int connectTimeout = -1;
    private int socketTimeout = -1;

    public int getConnectionRequestTimeout() {
        return connectionRequestTimeout;
    }

    public void setConnectionRequestTimeout(int connectionRequestTimeout) {
        this.connectionRequestTimeout = connectionRequestTimeout;
    }

    public int getConnectTimeout() {
        return connectTimeout;
    }

    public void setConnectTimeout(int connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    public int getSocketTimeout() {
        return socketTimeout;
    }

    public void setSocketTimeout(int socketTimeout) {
        this.socketTimeout = socketTimeout;
    }
}
