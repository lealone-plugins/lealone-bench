/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.bench.http;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.catalina.Context;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.Wrapper;
import org.apache.catalina.connector.Connector;
import org.apache.catalina.core.StandardVirtualThreadExecutor;
import org.apache.catalina.servlets.DefaultServlet;
import org.apache.catalina.startup.Tomcat;
import org.apache.coyote.http11.Http11NioProtocol;
import org.apache.coyote.http2.Http2Protocol;
import org.apache.tomcat.util.net.SSLHostConfig;
import org.apache.tomcat.util.net.SSLHostConfigCertificate;

import jakarta.servlet.ServletException;
import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

//加-Dstderr.encoding=UTF-8才不会有乱码
public class EmbedTomcatStart {

    // http://localhost:8080/test
    // https://localhost:8443/test
    // http://localhost:8080/index.html
    public static void main(String[] args) throws LifecycleException, IOException {
        long t1 = System.currentTimeMillis();
        Tomcat tomcat = new Tomcat();
        tomcat.setBaseDir("./target/tomcat");
        StandardVirtualThreadExecutor virtualThreadExecutor = new StandardVirtualThreadExecutor();
        virtualThreadExecutor.setName("virtual-thread-executor");
        tomcat.getService().addExecutor(virtualThreadExecutor);

        // setConnector(tomcat, virtualThreadExecutor);
        setSSLConnector(tomcat, virtualThreadExecutor);

        Context ctx = tomcat.addContext("", new File("./src/main/resources/http").getCanonicalPath());
        Wrapper wrapper = Tomcat.addServlet(ctx, "defaultServlet", new DefaultServlet());
        wrapper.addInitParameter("sendfileSize", "1"); // 按kb算
        ctx.addServletMappingDecoded("/", "defaultServlet");
        ctx.addWelcomeFile("index.html");

        Tomcat.addServlet(ctx, "testServlet", new TestServlet());
        ctx.addServletMappingDecoded("/test", "testServlet");

        tomcat.start();
        System.out.println("tomcat started, time: " + (System.currentTimeMillis() - t1) + " ms");
        tomcat.getServer().await();
    }

    public static void setConnector(Tomcat tomcat, StandardVirtualThreadExecutor virtualThreadExecutor) {
        Connector connector = new Connector(Http11NioProtocol.class.getName());
        connector.setPort(8080);
        connector.setService(tomcat.getService());
        connector.getProtocolHandler().setExecutor(virtualThreadExecutor);
        connector.getProtocolHandler().addUpgradeProtocol(new Http2Protocol());
        ((Http11NioProtocol) connector.getProtocolHandler()).setMaxKeepAliveRequests(100 * 10000);

        tomcat.getService().addConnector(connector);
        tomcat.setConnector(connector);
    }

    public static void setSSLConnector(Tomcat tomcat,
            StandardVirtualThreadExecutor virtualThreadExecutor) {
        Connector connector = new Connector(Http11NioProtocol.class.getName());
        connector.setPort(8443);
        connector.setService(tomcat.getService());
        connector.getProtocolHandler().setExecutor(virtualThreadExecutor);
        connector.getProtocolHandler().addUpgradeProtocol(new Http2Protocol());
        Http11NioProtocol protocol = (Http11NioProtocol) connector.getProtocolHandler();
        protocol.setMaxKeepAliveRequests(100 * 10000);

        protocol.setSSLEnabled(true); // 启用SSL
        SSLHostConfig config = new SSLHostConfig();
        SSLHostConfigCertificate certificate = new SSLHostConfigCertificate(config,
                SSLHostConfigCertificate.Type.RSA);
        certificate.setCertificateKeystoreFile(
                new File("src/main/resources/http/localhost-rsa.jks").getAbsolutePath());
        certificate.setCertificateKeyAlias("lealone-localhost");
        certificate.setCertificateKeystorePassword("123456");
        config.addCertificate(certificate);
        protocol.addSslHostConfig(config);

        tomcat.getService().addConnector(connector);
        tomcat.setConnector(connector);
    }

    static AtomicInteger count = new AtomicInteger();

    // http://localhost:8080/test
    public static class TestServlet extends HttpServlet {
        @Override
        protected void doPost(HttpServletRequest req, HttpServletResponse resp)
                throws ServletException, IOException {
            doGet(req, resp);
        }

        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp)
                throws ServletException, IOException {
            resp.setContentType("text/plain;charset=UTF-8");
            // java.io.PrintWriter out = resp.getWriter();
            // out.println("Hello Servlet");
            // out.flush();

            // StringBuffer buff = new StringBuffer();
            // for (int i = 0; i < 9000; i++) {
            // buff.append(i);
            // }
            // out.println(buff.toString());

            // resp.setTrailerFields(() -> {
            // Map<String, String> map = Map.of("k1", "v1", "k2", "v2");
            // return map;
            // });

            ServletOutputStream output = resp.getOutputStream();
            output.write(getTestData());
            // output.flush();

            // System.out.println("count: " + count.incrementAndGet());
        }
    }

    public static byte[] getTestData() {
        return bytes;
    }

    private static int size = 1024 * 1;
    private static byte[] bytes = new byte[size];
    static {
        for (int i = 0; i < size; i++) {
            bytes[i] = (byte) i;
        }
    }
}
