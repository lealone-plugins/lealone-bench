package com.lealone.plugins.bench.http.client;

import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.HttpEntity;

@SuppressWarnings("deprecation")
public class Http1GetExample {
    public static void main(String[] args) {
        // 1. 创建默认的 HttpClient 实例（默认支持 HTTP/1.1）
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {

            // 2. 创建 GET 请求对象，传入目标 URL
            HttpGet httpGet = new HttpGet("http://localhost:8080/test");
            for (int i = 0; i < 100; i++) {
                long t1 = System.currentTimeMillis();
                for (int n = 0; n < 1000; n++) {
                    // 3. 执行请求并获取响应
                    try (CloseableHttpResponse response = httpClient.execute(httpGet)) {

                        // 4. 获取并打印状态码和响应内容
                        // System.out.println("响应状态码: " + response.getCode());

                        HttpEntity entity = response.getEntity();
                        if (entity != null) {
                            // System.out
                            // .println("Response Body: " + EntityUtils.toString(entity).length());
                            // String responseBody = EntityUtils.toString(entity);
                            // System.out.println("响应内容: " + responseBody);
                        }
                    }
                }
                System.out.println("time: " + (System.currentTimeMillis() - t1));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
