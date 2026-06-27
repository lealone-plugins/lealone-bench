package com.lealone.plugins.bench.http.client;

import javax.net.ssl.SSLContext;

import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.client5.http.ssl.SSLConnectionSocketFactory;
import org.apache.hc.client5.http.ssl.SSLConnectionSocketFactoryBuilder;
import org.apache.hc.core5.ssl.SSLContexts;

@SuppressWarnings("deprecation")
public class Https1GetExample {

    public static void main(String[] args) throws Exception {

        // 1. 创建一个信任所有证书的 SSLContext
        SSLContext sslContext = SSLContexts.custom().loadTrustMaterial(null, (chain, authType) -> true)
                .build();

        // 2. 使用 Builder 创建 SSLConnectionSocketFactory
        SSLConnectionSocketFactory sslSocketFactory = SSLConnectionSocketFactoryBuilder.create()
                .setSslContext(sslContext).setHostnameVerifier(NoopHostnameVerifier.INSTANCE) // 禁用主机名验证
                .build();

        // 3. 使用 Builder 创建 ConnectionManager，并设置 SSL factory
        HttpClientConnectionManager connectionManager = PoolingHttpClientConnectionManagerBuilder
                .create().setSSLSocketFactory(sslSocketFactory).build();

        // 4. 使用自定义的 ConnectionManager 构建 HttpClient
        try (CloseableHttpClient httpClient = HttpClients.custom()
                .setConnectionManager(connectionManager).build()) {

            // 5. 创建 GET 请求并执行
            HttpGet httpGet = new HttpGet("https://localhost:8443/test");

            for (int i = 0; i < 100; i++) {
                long t1 = System.currentTimeMillis();
                for (int n = 0; n < 1000; n++) {
                    try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
                        // System.out.println("Response Code: " + response.getCode());
                        // String responseBody = EntityUtils.toString(response.getEntity());
                        // System.out.println("Response Body: " + responseBody);
                    }
                }
                System.out.println("time: " + (System.currentTimeMillis() - t1));
            }
        }
    }
}
