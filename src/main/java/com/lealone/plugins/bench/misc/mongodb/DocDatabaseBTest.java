/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.bench.misc.mongodb;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.client.model.Filters;

public abstract class DocDatabaseBTest {

    public static final int LEALONE_PORT = 9310;
    public static final int MONGODB_PORT = 27017;

    public static final AtomicInteger id = new AtomicInteger();

    final Random random = new Random();
    final int clientCount = 2; // 超过cpu核数性能会下降
    final int rowCount = 10000; // 总记录数

    int threadCount = 48;
    int outerLoop = 30;
    int innerLoop = 200;
    String operation;

    void beforeBenchTest() {
    }

    void afterBenchTest() {
    }

    String getConnectionString(int port) {
        String connectionString = "mongodb://127.0.0.1:" + port;
        connectionString += "/?maxPoolSize=" + threadCount + "&&minPoolSize=" + (threadCount / 2);
        return connectionString;
    }

    abstract void createMongoClients(int port);

    abstract void closeMongoClients();

    void run(int port) {
        createMongoClients(port);
        beforeBenchTest();
        for (int i = 0; i < outerLoop; i++) {
            benchTest();
        }
        afterBenchTest();
        closeMongoClients();
    }

    abstract long benchTest(int clientIndex);

    void benchTest() {
        AtomicLong totalTime = new AtomicLong();
        // 通过Executors.newFixedThreadPool的方式执行也没有比启动新线程好太多
        Thread[] threads = new Thread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            int index = i;
            threads[i] = new Thread(() -> {
                long time = benchTest(index);
                totalTime.addAndGet(time);
                String tn = "Thread-";
                if (index < 10)
                    tn = "Thread-0";
                System.out.println(tn + index + ", document count: " + (innerLoop) + ", " + operation
                        + " document time: " + TimeUnit.NANOSECONDS.toMillis(time) + " ms");
            });
        }
        long t1 = System.currentTimeMillis();
        for (int i = 0; i < threadCount; i++) {
            threads[i].start();
        }
        for (int i = 0; i < threadCount; i++) {
            try {
                threads[i].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        long t2 = System.currentTimeMillis();
        long total = t2 - t1;
        long avg = totalTime.get() / 1000 / 1000 / threadCount;
        System.out.println(getClass().getSimpleName() + " thread count: " + (threadCount)
                + ", document count: " + (threadCount * innerLoop) + ", total time: " + total + " ms"
                + ", avg time: " + avg + " ms");
    }

    public static void testDocument() {
        Document doc = Document.parse("{ status: { $in: [ \"A\", \"D\" ] } }");
        System.out.println(doc.toJson());

        Bson bson = Filters.and(Filters.eq("_id", 1), Filters.eq("_id", 2));
        System.out.println(bson.toBsonDocument().toJson());

        bson = Filters.in("_id", 1, 2, 2);
        System.out.println(bson.toBsonDocument().toJson());
    }
}
