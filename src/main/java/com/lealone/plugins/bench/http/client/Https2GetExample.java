package com.lealone.plugins.bench.http.client;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;

import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.client5.http.async.methods.SimpleHttpResponse;
import org.apache.hc.client5.http.async.methods.SimpleRequestBuilder;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.client5.http.impl.async.HttpAsyncClients;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManager;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.client5.http.ssl.TrustSelfSignedStrategy;
import org.apache.hc.core5.concurrent.FutureCallback;
import org.apache.hc.core5.http.nio.ssl.TlsStrategy;
import org.apache.hc.core5.ssl.SSLContexts;

public class Https2GetExample {

    public static void main(String[] args) throws Exception {
        // 1. 配置 SSL 上下文 (HTTP/2 强制要求 HTTPS)
        SSLContext sslContext = SSLContexts.custom()
                .loadTrustMaterial(null, new TrustSelfSignedStrategy()) // 测试环境可信任自签名证书
                .build();

        // 2. 配置 TLS 策略
        TlsStrategy tlsStrategy = ClientTlsStrategyBuilder.create().setSslContext(sslContext)
                .setHostnameVerifier(NoopHostnameVerifier.INSTANCE) // 核心修改：跳过主机名校验
                .build();

        // 3. 配置异步连接管理器以支持 HTTP/2 多路复用
        PoolingAsyncClientConnectionManager connManager = PoolingAsyncClientConnectionManagerBuilder
                .create().setTlsStrategy(tlsStrategy).build();

        // 4. 构建并启动异步 HTTP 客户端
        CloseableHttpAsyncClient client = HttpAsyncClients.custom().setConnectionManager(connManager)
                .build();

        client.start();

        try {
            client.start();

            // 3. 构建 GET 请求（使用 HTTPS）
            SimpleHttpRequest request = SimpleRequestBuilder.get("https://localhost:8443/test").build();

            for (int i = 0; i < 100; i++) {
                long t1 = System.currentTimeMillis();
                CountDownLatch latch = new CountDownLatch(1000);
                for (int n = 0; n < 1000; n++) {
                    Future<?> f = null;
                    // 4. 执行请求
                    f = client.execute(request, new FutureCallback<SimpleHttpResponse>() {
                        @Override
                        public void completed(SimpleHttpResponse response) {
                            // 关键：打印实际使用的协议版本
                            // System.out.println("Negotiated Protocol: " + response.getVersion());
                            // // 如果输出为 HTTP/2.0，则说明成功使用 HTTP/2
                            // System.out.println("Status: " + response.getCode());
                            // System.out.println("Body preview: " + response.getBodyText().substring(0,
                            // Math.min(100, response.getBodyText().length())) + "...");
                            latch.countDown();
                        }

                        @Override
                        public void failed(Exception ex) {
                            System.err.println("Request failed: " + ex.getMessage());
                            latch.countDown();
                        }

                        @Override
                        public void cancelled() {
                            System.out.println("Request cancelled");
                            latch.countDown();
                        }
                    });
                    f.get();

                }
                // 等待异步请求完成（最多等待10秒）
                latch.await(100, TimeUnit.SECONDS);
                System.out.println("time: " + (System.currentTimeMillis() - t1));
            }
        } finally {
            client.close();
        }
    }
}
