package com.alibaba.datax.plugin.reader.httpreader;

import java.io.IOException;
import java.net.URL;
import java.util.Map;
import java.util.Optional;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.httpreader.enums.AuthTypeEnum;
import com.alibaba.datax.plugin.reader.httpreader.enums.HttpMethodEnum;
import org.apache.http.Consts;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import static com.alibaba.datax.plugin.reader.httpreader.Constant.DEFAULT_POOL_SIZE;

/**
 * <p>
 * HTTP 请求工具类
 * </p>
 *
 * @author JupiterMouse 2020/8/5
 * @since 1.0
 */
public class HttpReaderClientUtil {

    private final Configuration conf;

    private CredentialsProvider provider = null;

    private CloseableHttpClient httpClient;


    private String url = null;

    private String method = null;

    private Map<String, String> headers = null;

    private Map<String, String> auth = null;

    private String payload = null;

    private Connect connect = null;

    public HttpReaderClientUtil(Configuration conf) {
        this.conf = conf;
    }

    public String sendRequest() {
        initApacheHttpClient(this.conf);
        String response = null;
        if (HttpMethodEnum.GET.name().equalsIgnoreCase(this.method)) {
            response = this.get(this.url, this.payload, this.headers);
        } else if (HttpMethodEnum.POST.name().equalsIgnoreCase(this.method)) {
            response = this.post(this.url, this.payload, this.headers);
        }
        destroy();
        return response;
    }

    private void initApacheHttpClient(Configuration conf) {
        this.url = conf.getString(Key.URL);
        this.method = conf.getString(Key.METHOD);
        this.payload = conf.getString(Key.PAYLOAD);
        this.headers = conf.getMap(Key.HEADERS, String.class);
        this.auth = conf.getMap(Key.AUTH, String.class);
        this.connect = Optional.ofNullable(this.conf.get(Key.MAP_CONNECTOR, Connect.class)).orElse(new Connect());

        RequestConfig requestConfig = RequestConfig.custom()
                .setSocketTimeout(this.connect.getSocketTimeout())
                .setConnectTimeout(this.connect.getConnectTimeout())
                .setConnectionRequestTimeout(this.connect.getConnectionRequestTimeout())
                .build();
        this.setAuth(this.auth);
        if (null == provider) {
            httpClient = HttpClientBuilder.create().setMaxConnTotal(DEFAULT_POOL_SIZE).setMaxConnPerRoute(DEFAULT_POOL_SIZE)
                    .setDefaultRequestConfig(requestConfig).build();
        } else {
            httpClient = HttpClientBuilder.create().setMaxConnTotal(DEFAULT_POOL_SIZE).setMaxConnPerRoute(DEFAULT_POOL_SIZE)
                    .setDefaultRequestConfig(requestConfig).setDefaultCredentialsProvider(provider).build();
        }
    }

    private void setAuth(Map<String, String> auth) {
        if (auth == null) {
            return;
        }
        if (AuthTypeEnum.BASIC.name().equalsIgnoreCase(auth.get(Key.AUTH_TYPE))) {
            provider = new BasicCredentialsProvider();
            String username = auth.get(Key.USERNAME);
            String password = auth.get(Key.PASSWORD);
            provider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
        }
    }

    private void destroy() {
        destroyApacheHttpClient();
    }

    private void destroyApacheHttpClient() {
        try {
            if (httpClient != null) {
                httpClient.close();
                httpClient = null;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String get(String url, String payload, Map<String, String> headers) {
        HttpGet httpGet = new HttpGet();
        headers.forEach(httpGet::addHeader);
        String response = null;
        try {
            url = url + payload;
            URL rUrl = new URL(url);
            httpGet.setURI(rUrl.toURI());
            response = this.executeAndGet(httpGet);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return response;
    }

    private String post(String url, String payload, Map<String, String> headers) {
        HttpPost httpPost = new HttpPost();
        headers.forEach(httpPost::addHeader);
        String response = null;
        try {
            URL rUrl = new URL(url);
            httpPost.setURI(rUrl.toURI());
            httpPost.setEntity(new StringEntity(payload));
            response = this.executeAndGet(httpPost);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return response;
    }

    private String executeAndGet(HttpRequestBase httpRequestBase) throws Exception {
        HttpResponse response;
        String entiStr = "";
        try {
            response = httpClient.execute(httpRequestBase);

            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                System.err.println("请求地址：" + httpRequestBase.getURI() + ", 请求方法：" + httpRequestBase.getMethod()
                        + ",STATUS CODE = " + response.getStatusLine().getStatusCode());
                httpRequestBase.abort();
                throw new Exception("Response Status Code : " + response.getStatusLine().getStatusCode());
            } else {
                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    entiStr = EntityUtils.toString(entity, Consts.UTF_8);
                } else {
                    throw new Exception("Response Entity Is Null");
                }
            }
        } catch (Exception e) {
            throw e;
        }

        return entiStr;
    }

}
