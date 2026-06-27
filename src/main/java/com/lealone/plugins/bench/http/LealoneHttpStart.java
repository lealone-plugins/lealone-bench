/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.bench.http;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.lealone.common.exceptions.ConfigException;
import com.lealone.main.Lealone;
import com.lealone.server.http.HttpRouter;
import com.lealone.server.http.HttpServerEngine;
import com.lealone.server.servlet.RequestDispatcher;
import com.lealone.server.servlet.ServletException;
import com.lealone.server.servlet.ServletInputStream;
import com.lealone.server.servlet.ServletOutputStream;
import com.lealone.server.servlet.http.HttpServlet;
import com.lealone.server.servlet.http.HttpServletRequest;
import com.lealone.server.servlet.http.HttpServletResponse;
import com.lealone.sql.config.Config;
import com.lealone.sql.config.Config.PluggableEngineDef;
import com.lealone.sql.config.ConfigListener;

public class LealoneHttpStart extends HttpRouter implements ConfigListener {

    // http://localhost:8080/index.html
    public static void main(String[] args) {
        System.setProperty("lealone.config.listener", LealoneHttpStart.class.getName());
        Lealone.main(args);
    }

    @Override
    public void init(Map<String, String> config) {
        super.init(config);
        httpServer.addServlet("testServlet", new TestServlet());
        httpServer.addServletMappingDecoded("/test", "testServlet");
        httpServer.addServlet("testDispatchServlet", new TestDispatchServlet());
        httpServer.addServletMappingDecoded("/testDispatch", "testDispatchServlet");
    }

    @Override
    public void applyConfig(Config config) throws ConfigException {
        for (PluggableEngineDef e : config.protocol_server_engines) {
            if (HttpServerEngine.NAME.equalsIgnoreCase(e.name)) {
                e.enabled = true;
            }
        }
    }

    static AtomicInteger count = new AtomicInteger();

    public static class TestDispatchServlet extends HttpServlet {

        @Override
        protected void doPost(HttpServletRequest req, HttpServletResponse resp)
                throws ServletException, IOException {
            doGet(req, resp);
        }

        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp)
                throws ServletException, IOException {
            // resp.sendRedirect("/test");
            RequestDispatcher dispatcher = req.getRequestDispatcher("/test");
            dispatcher.forward(req, resp);
        }
    }

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
            // if (!req.isAsyncStarted())
            // req.startAsync();
            // req.getAsyncContext().start(() -> {
            // try {
            // run(req, resp);
            // req.getAsyncContext().complete();
            // } catch (ServletException e) {
            // e.printStackTrace();
            // } catch (IOException e) {
            // e.printStackTrace();
            // }
            // });
            run(req, resp);
        }

        private void run(HttpServletRequest req, HttpServletResponse resp)
                throws ServletException, IOException {
            resp.setContentType("text/plain;charset=UTF-8");
            ServletInputStream input = req.getInputStream();
            input.read();
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
            // WriteListener wl = new WriteListener() {
            // @Override
            // public void onWritePossible() throws IOException {
            // }
            //
            // @Override
            // public void onError(Throwable throwable) {
            // }
            // };
            // output.setWriteListener(wl);
            output.write(EmbedTomcatStart.getTestData());
            output.flush();

            // System.out.println("count: " + count.incrementAndGet());
        }
    }
}
