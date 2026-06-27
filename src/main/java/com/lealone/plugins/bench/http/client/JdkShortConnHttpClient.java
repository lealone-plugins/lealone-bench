package com.lealone.plugins.bench.http.client;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

// -Djdk.httpclient.allowRestrictedHeaders=Connection,Content-Length
public class JdkShortConnHttpClient {

    public static void main(String[] args) throws Exception {
        // 不起作用
        // System.setProperty("jdk.httpclient.allowRestrictedHeaders", "Connection,Content-Length");
        int count = 8;
        Thread[] threads = new Thread[count];
        for (int i = 0; i < count; i++) {
            threads[i] = new Thread(() -> {
                try {
                    run();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            threads[i].start();
        }
        for (int i = 0; i < count; i++) {
            threads[i].join();
        }
        client.close();
    }

    // 1. 创建 HttpClient
    public static HttpClient client = HttpClient.newBuilder()
            .connectTimeout(java.time.Duration.ofSeconds(10000)).build();

    public static void run() throws Exception {
        // 2. 构建请求
        HttpRequest request = HttpRequest.newBuilder().uri(URI.create("http://localhost:8080/test"))
                .GET() // 默认就是GET，可省略
                .header("Content-Type", "text/plain") //
                .header("Connection", "close") // 核心：告知服务端用完断开
                .timeout(java.time.Duration.ofSeconds(100000)) // 10秒超时
                .build();
        for (int i = 0; i < 100; i++) {
            long t1 = System.currentTimeMillis();
            for (int n = 0; n < 1000; n++) {
                // 3. 发送同步请求，阻塞等待响应
                @SuppressWarnings("unused")
                HttpResponse<String> response = client.send(request,
                        HttpResponse.BodyHandlers.ofString());

                // 4. 打印结果
                // System.out.println("响应状态码：" + response.statusCode());
                // System.out.println("响应头：" + response.headers());
                // System.out.println("响应体：" + response.body());
            }
            System.out.println("time: " + (System.currentTimeMillis() - t1));
        }
    }
}
