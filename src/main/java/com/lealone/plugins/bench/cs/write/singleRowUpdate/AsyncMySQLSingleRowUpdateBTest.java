/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.bench.cs.write.singleRowUpdate;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.mysqlclient.MySQLBuilder;
import io.vertx.mysqlclient.MySQLConnectOptions;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.SqlClient;

public class AsyncMySQLSingleRowUpdateBTest {

    protected static long toMillis(long duration) {
        return TimeUnit.NANOSECONDS.toMillis(duration);
    }

    static int sqlCount = 200;
    static int threadCount = 16;

    public static void main(String[] args) throws InterruptedException {
        VertxOptions vertxOptions = new VertxOptions();
        vertxOptions.setEventLoopPoolSize(threadCount);
        vertxOptions.setBlockedThreadCheckInterval(60 * 60 * 1000);
        Vertx vertx = Vertx.vertx(vertxOptions);
        SqlClient[] clients = new SqlClient[threadCount];
        clients[0] = getSqlClient(vertx);
        for (int i = 0; i < threadCount; i++) {
            // clients[i] = getSqlClient();
            clients[i] = clients[0]; // 每个线程一个SqlClient或共用一个SqlClient，性能没区别
        }
        for (int n = 0; n < 260; n++) {
            Thread[] threads = new Thread[threadCount];
            Test[] tests = new Test[threadCount];
            for (int i = 0; i < threadCount; i++) {
                tests[i] = new Test(clients[i], i + 10);
                threads[i] = new Thread(tests[i]);
            }
            long t1 = System.currentTimeMillis();
            for (int i = 0; i < threadCount; i++) {
                // threads[i].start();
                vertx.deployVerticle(tests[i]);
            }
            long totalTime = 0;
            for (int i = 0; i < threadCount; i++) {
                // threads[i].join();
                tests[i].await();
                totalTime += tests[i].getTotalTime();
            }
            long t2 = System.currentTimeMillis();
            long avgTime = toMillis(totalTime / threadCount);
            totalTime = (t2 - t1);

            System.out.println("AsyncMySQLSingleRowUpdateBTest sql count: " + sqlCount * threadCount
                    + ", thread count: " + threadCount + ", avg time: " + avgTime + " ms"
                    + ", total time: " + totalTime + " ms");
        }
        for (int i = 0; i < threadCount; i++) {
            clients[i].close();
        }
        vertx.close();
    }

    public static SqlClient getSqlClient(Vertx vertx) {
        MySQLConnectOptions connectOptions = new MySQLConnectOptions().setPort(3306).setHost("localhost")
                .setDatabase("test").setUser("test").setPassword("test");

        // connectOptions = new MySQLConnectOptions().setPort(9310).setHost("localhost")
        // .setDatabase("mysql").setUser("root").setPassword("");

        // Pool options
        PoolOptions poolOptions = new PoolOptions().setMaxSize(threadCount * 2);
        poolOptions.setConnectionTimeout(120 * 1000);
        // poolOptions.setShared(true);
        // poolOptions.setEventLoopSize(4);
        // Create the client pool
        SqlClient client = MySQLBuilder.client().using(vertx).with(poolOptions)
                .connectingTo(connectOptions).build();
        return client;
    }

    public static class Test extends AbstractVerticle implements Runnable {
        long startTime;
        long endTime;
        SqlClient client;
        Random random = new Random();
        int rowCount = 10000;
        int id;
        CountDownLatch latch = new CountDownLatch(1);

        public Test(SqlClient client, int id) {
            this.client = client;
            this.id = id;
        }

        public long getTotalTime() {
            return endTime - startTime;
        }

        @Override
        public void start() throws Exception {
            run();
        }

        public void await() {
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void run() {
            AtomicInteger counter = new AtomicInteger(sqlCount);
            startTime = System.nanoTime();
            for (int i = 0; i < sqlCount; i++) {
                int pk = random.nextInt(rowCount);
                int f1 = pk * 10;
                String sql = "update SingleRowUpdateBTest set f1=" + f1 + " where pk=" + pk;

                // sql = "select * from SingleRowUpdateBTest where pk=" + pk;

                client.query(sql).execute().onComplete(ar -> {
                    if (!ar.succeeded()) {
                        System.out.println("Failure: " + ar.cause().getMessage());
                    }
                    if (counter.decrementAndGet() == 0) {
                        endTime = System.nanoTime();
                        latch.countDown();
                    }
                });
            }
            // System.out.println("Thread-" + id + " sql count: " + sqlCount + ", time: "
            // + (endTime - startTime) / 1000 / 1000 + " ms");
        }
    }
}
