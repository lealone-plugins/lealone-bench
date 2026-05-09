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

import com.mysql.cj.xdevapi.Session;
import com.mysql.cj.xdevapi.SessionFactory;

public class AsyncMySQLSingleRowUpdateBTest2 {

    protected static long toMillis(long duration) {
        return TimeUnit.NANOSECONDS.toMillis(duration);
    }

    static int sqlCount = 100;
    static int threadCount = 16;
    static int outerLoop = 15;

    public static void main(String[] args) throws Exception {
        Session[] sessions = new Session[threadCount];
        for (int i = 0; i < threadCount; i++) {
            sessions[i] = getSession();
        }

        for (int i = 0; i < outerLoop; i++) {
            long t1 = System.currentTimeMillis();
            run(threadCount, sessions);
            long t2 = System.currentTimeMillis();
            System.out.println("AsyncMySQLSingleRowUpdateBTest2 sql count: "
                    + (outerLoop * threadCount * sqlCount) + ", total time: " + (t2 - t1) + " ms");
        }

        for (int i = 0; i < threadCount; i++) {
            sessions[i].close();
        }
    }

    public static void run(int threadCount, Session[] sessions) throws Exception {
        for (int n = 0; n < 15; n++) {
            Thread[] threads = new Thread[threadCount];
            Test[] tests = new Test[threadCount];
            for (int i = 0; i < threadCount; i++) {
                tests[i] = new Test(sessions[i], i + 10);
                threads[i] = new Thread(tests[i]);
            }
            long t1 = System.currentTimeMillis();
            for (int i = 0; i < threadCount; i++) {
                threads[i].start();
            }
            long totalTime = 0;
            for (int i = 0; i < threadCount; i++) {
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
    }

    public static Session getSession() {
        String CONNECTION_URI = "mysqlx://test:test@localhost:33060/test";
        return new SessionFactory().getSession(CONNECTION_URI);
    }

    public static class Test implements Runnable {
        long startTime;
        long endTime;
        Session client;
        Random random = new Random();
        int rowCount = 10000;
        int id;
        CountDownLatch latch = new CountDownLatch(1);

        public Test(Session client, int id) {
            this.client = client;
            this.id = id;
        }

        public long getTotalTime() {
            return endTime - startTime;
        }

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

                client.sql(sql).executeAsync().thenAccept(ar -> {
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
