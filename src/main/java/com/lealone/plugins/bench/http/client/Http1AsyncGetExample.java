package com.lealone.plugins.bench.http.client;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.client5.http.async.methods.SimpleHttpResponse;
import org.apache.hc.client5.http.async.methods.SimpleRequestBuilder;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.client5.http.impl.async.HttpAsyncClients;
import org.apache.hc.core5.concurrent.FutureCallback;

//虽然是异步api，但还是处理完一个收到响应了再发下一个
public class Http1AsyncGetExample {
    public static void main(String[] args) throws Exception {
        // 1. 创建并启动异步 HTTP 客户端
        CloseableHttpAsyncClient httpClient = HttpAsyncClients.createDefault();
        httpClient.start();

        try {
            // 2. 构建 GET 请求
            SimpleHttpRequest request = SimpleRequestBuilder.get("http://localhost:8080/test").build();
            for (int i = 0; i < 300; i++) {
                long t1 = System.currentTimeMillis();
                CountDownLatch latch = new CountDownLatch(1000);
                for (int n = 0; n < 1000; n++) {
                    // 3. 异步执行请求，并通过 FutureCallback 处理响应
                    httpClient.execute(request, new FutureCallback<SimpleHttpResponse>() {
                        @Override
                        public void completed(SimpleHttpResponse response) {
                            response.getBodyText();
                            // 请求成功完成
                            // System.out.println("响应状态码: " + response.getCode());
                            // System.out.println("响应内容: " + response.getBodyText());
                            latch.countDown();
                        }

                        @Override
                        public void failed(Exception ex) {
                            // 请求失败
                            System.err.println("请求失败: " + ex.getMessage());
                            latch.countDown();
                        }

                        @Override
                        public void cancelled() {
                            // 请求被取消
                            System.out.println("请求已取消");
                            latch.countDown();
                        }
                    });
                }
                // 等待异步请求完成（最多等待10秒）
                latch.await(100, TimeUnit.SECONDS);
                System.out.println("time: " + (System.currentTimeMillis() - t1));
            }
        } finally {
            // 5. 关闭客户端释放资源
            httpClient.close();
        }
    }
}
