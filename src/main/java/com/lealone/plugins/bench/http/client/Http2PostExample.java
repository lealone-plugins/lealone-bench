package com.lealone.plugins.bench.http.client;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.client5.http.async.methods.SimpleHttpResponse;
import org.apache.hc.client5.http.async.methods.SimpleRequestBuilder;
import org.apache.hc.client5.http.async.methods.SimpleRequestProducer;
import org.apache.hc.client5.http.async.methods.SimpleResponseConsumer;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.client5.http.impl.async.H2AsyncClientBuilder;
import org.apache.hc.core5.concurrent.FutureCallback;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.reactor.IOReactorConfig;

public class Http2PostExample {

    public static void main(String[] args) throws Exception {
        int threadCount = 1;
        IOReactorConfig ioReactorConfig = IOReactorConfig.custom().setIoThreadCount(1).build();
        CloseableHttpAsyncClient[] httpClients = new CloseableHttpAsyncClient[threadCount];

        for (int i = 0; i < threadCount; i++) {
            httpClients[i] = H2AsyncClientBuilder.create().setIOReactorConfig(ioReactorConfig).build();
            // httpClients[i].start();
        }

        // CloseableHttpAsyncClient httpClient = HttpAsyncClients.createHttp2Default();
        // httpClient.start();
        for (int n = 0; n < 200; n++) {
            long t1 = System.currentTimeMillis();
            Thread[] threads = new Thread[threadCount];
            for (int i = 0; i < threadCount; i++) {
                int index = i;
                threads[i] = new Thread(() -> {
                    try {
                        run(httpClients[index]);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
                threads[i].start();
            }
            for (int i = 0; i < threadCount; i++) {
                threads[i].join();
            }
            System.out.println("total time: " + (System.currentTimeMillis() - t1) + " completed: "
                    + completed + " failed: " + failed);
            completed = failed = 0;
        }
        for (int i = 0; i < threadCount; i++) {
            httpClients[i].close();
        }
    }

    static int completed;
    static int failed;

    @SuppressWarnings("unused")
    public static void run(CloseableHttpAsyncClient httpClient) throws Exception {
        // 1. 创建支持 HTTP/2 的异步客户端
        // try (CloseableHttpAsyncClient httpClient = HttpAsyncClients.createHttp2Default()) {
        // 启动客户端
        // httpClient.start();

        // tomcat还有bug，要反复启动关掉httpClient才行
        IOReactorConfig ioReactorConfig = IOReactorConfig.custom().setIoThreadCount(1).build();
        httpClient = H2AsyncClientBuilder.create().setIOReactorConfig(ioReactorConfig).build();
        httpClient.start();

        // 2. 构建一个简单的 POST 请求
        // SimpleHttpRequest request = SimpleRequestBuilder.get("http://localhost:8080/test").build();

        final SimpleHttpRequest request = SimpleRequestBuilder.post("http://localhost:8080/test")
                .setBody("some stuff", ContentType.TEXT_PLAIN).build();
        // 用于在异步回调完成前阻塞主线程
        for (int i = 0; i < 1; i++) {
            long t1 = System.currentTimeMillis();
            int count = 1000;
            CountDownLatch latch = new CountDownLatch(count);
            for (int n = 0; n < count; n++) {
                // int inidex = n;
                Future<?> f = null;
                // 3. 执行请求
                f = httpClient.execute(SimpleRequestProducer.create(request),
                        SimpleResponseConsumer.create(), new FutureCallback<SimpleHttpResponse>() {
                            @Override
                            public void completed(SimpleHttpResponse response) {
                                completed++;
                                // System.out.println("inidex: " + inidex);

                                // 请求成功，打印响应内容和协议版本
                                // System.out.println("HTTP Version: " + response.getVersion());
                                // System.out.println("Response Body: " + response.getBodyText());
                                latch.countDown();
                            }

                            @Override
                            public void failed(Exception ex) {
                                failed++;
                                // 请求失败，打印异常
                                System.err.println("Request failed: " + ex.getMessage());
                                latch.countDown();
                            }

                            @Override
                            public void cancelled() {
                                System.out.println("Request cancelled");
                                latch.countDown();
                            }
                        });
                // f.get();
            }
            // 等待异步请求完成（最多等待10秒）
            latch.await(10, TimeUnit.SECONDS);
            // System.out.println("time: " + (System.currentTimeMillis() - t1));
            // }
        }
        httpClient.close();
    }
}
