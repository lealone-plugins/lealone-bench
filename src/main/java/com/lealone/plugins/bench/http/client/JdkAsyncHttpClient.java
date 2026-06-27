package com.lealone.plugins.bench.http.client;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

public class JdkAsyncHttpClient {
    public static void main(String[] args) throws InterruptedException {
        HttpClient client = HttpClient.newBuilder().connectTimeout(java.time.Duration.ofSeconds(10000))
                .build();

        HttpRequest request = HttpRequest.newBuilder().uri(URI.create("http://localhost:8080/test"))
                .GET() // 默认就是GET，可省略
                .header("Content-Type", "text/plain") //
                .timeout(java.time.Duration.ofSeconds(10000)) // 10秒超时
                .build();
        for (int i = 0; i < 100; i++) {
            int count = 1000;
            CountDownLatch latch = new CountDownLatch(count);
            long t1 = System.currentTimeMillis();
            for (int n = 0; n < count; n++) {
                // 异步发送，返回CompletableFuture
                CompletableFuture<HttpResponse<String>> future = client.sendAsync(request,
                        HttpResponse.BodyHandlers.ofString());
                // 回调处理响应
                future.thenAccept(response -> {
                    // System.out.println("异步状态码：" + response.statusCode());
                    // System.out.println("异步返回内容：" + response.body());
                    latch.countDown();
                }).exceptionally(ex -> {
                    // System.err.println("请求异常：" + ex.getMessage());
                    ex.printStackTrace();
                    latch.countDown();
                    return null;
                });
            }
            latch.await();
            System.out.println("time: " + (System.currentTimeMillis() - t1));
        }
        client.close();
    }
}
