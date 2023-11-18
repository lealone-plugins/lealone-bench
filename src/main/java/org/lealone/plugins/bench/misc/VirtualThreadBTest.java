/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.bench.misc;

import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.IntStream;

import org.lealone.client.jdbc.JdbcStatement;
import org.lealone.plugins.bench.cs.ClientServerBTest;

// javac --enable-preview -source 19 VirtualThreadBTest.java
// java --enable-preview -cp . VirtualThreadBTest
// java --enable-preview --source 19 VirtualThreadBTest.java
public class VirtualThreadBTest {

    public static void main(String[] args) throws Exception {
        // testLock();

        ReentrantLock lock0 = new ReentrantLock();
        ThreadFactory factory = Thread.ofVirtual().name("vt-", 1).factory();
        Thread t = factory.newThread(() -> {
            lock0.lock();
            System.out.println(Thread.currentThread().getName());
            System.out.println(Thread.currentThread().isVirtual());
            lock0.unlock();
        });
        t.start();
        lock0.lock();
        t.getState();
        t.getState();
        t.getState();
        lock0.unlock();
        t = factory.newThread(() -> {
            System.out.println(Thread.currentThread().getName());
            System.out.println(Thread.currentThread().isVirtual());
        });
        t.start();
        t = factory.newThread(() -> {
            System.out.println(Thread.currentThread().getName());
        });
        t.start();
        // Executors.newThreadPerTaskExecutor(factory);
        // Executors.newFixedThreadPool(1, factory);
        try (var executor = Executors.newFixedThreadPool(1, factory)) {
            for (int i = 0; i < 100; i++) {
                executor.submit(() -> {
                    System.out.println(Thread.currentThread().isVirtual());
                    return null;
                });
            }
        }

        ReentrantLock lock = new ReentrantLock();
        AtomicInteger int1 = new AtomicInteger(1);
        ThreadFactory factory1 = Thread.ofVirtual().name("vt-", 1).factory();
        // 这两个Executor都会导致虚拟线程遇到ReentrantLock后挂起系统线程
        // 只有newThreadPerTaskExecutor是正确的
        // var executor0 = Executors.newFixedThreadPool(2, factory1);
        // Executors.newSingleThreadScheduledExecutor(factory1)
        try (var executor1 = Executors.newThreadPerTaskExecutor(factory1)) {
            for (int i = 0; i < 16; i++) {
                executor1.submit(() -> {
                    System.out.println("e1：" + int1.getAndIncrement());
                    lock.lock();
                    System.out.println(Thread.currentThread().getName());
                    System.out.println(Thread.currentThread().isVirtual());
                    lock.unlock();
                    System.out.println("e1：end " + int1.get());
                    return null;
                });
            }
            lock.lock();
            lock.unlock();
        }

        AtomicInteger int2 = new AtomicInteger(1);
        ThreadFactory factory2 = Thread.ofVirtual().name("vt-", 1).factory();
        var executor2 = Executors.newFixedThreadPool(1, factory2);

        for (int i = 0; i < 100; i++) {
            executor2.submit(() -> {
                System.out.println("e2：" + int2.getAndIncrement());
                lock.lock();
                System.out.println(Thread.currentThread().getName());
                System.out.println(Thread.currentThread().isVirtual());
                lock.unlock();
                System.out.println("e2：end " + int2.get());
                return null;
            });
        }
        // executor1.shutdown();
        executor2.shutdown();
    }

    public static void main4(String[] args) throws Exception {
        Connection conn = ClientServerBTest.getLealoneConnection(true);
        // h2和mysql的jdbc client都是基于synchronized来实现的
        // 所以调用insertVirtualThread反而比调用insertSync
        // 虚拟线程遇到synchronized会阻塞系统线程
        // Connection conn = ClientServerBTest.getH2Connection();
        // Connection conn = ClientServerBTest.getMySQLConnection();
        Statement stmt = conn.createStatement();
        init(stmt);

        int rowCount = 5000;
        for (int i = 0; i < 50; i++) {
            // insertSync(stmt, rowCount);
            // insertAsync(stmt, rowCount);
            insertVirtualThread(stmt, rowCount);
            // insertVirtualThreadAsync(stmt, rowCount);
        }

        // query(stmt);

        stmt.close();
        conn.close();
    }

    static void insertSync(Statement stmt, int rowCount) throws Exception {
        long t1 = System.currentTimeMillis();
        for (int i = 0; i < rowCount; i++) {
            stmt.executeUpdate("INSERT INTO test VALUES('a', 1, 10)");
        }
        long t2 = System.currentTimeMillis();
        System.out.println("Sync: row count: " + rowCount + ", time: " + (t2 - t1) + " ms");
    }

    static void insertAsync(Statement stmt, int rowCount) throws Exception {
        JdbcStatement stmtAsync = (JdbcStatement) stmt;
        CountDownLatch latch = new CountDownLatch(1);
        long t1 = System.currentTimeMillis();
        AtomicLong endTime = new AtomicLong();
        AtomicLong count = new AtomicLong(rowCount);
        for (int i = 0; i < rowCount; i++) {
            stmtAsync.executeUpdateAsync("INSERT INTO test VALUES('a', 1, 10)").onComplete(ar -> {
                if (count.decrementAndGet() == 0) {
                    endTime.set(System.currentTimeMillis());
                    latch.countDown();
                }
            });
        }
        latch.await();
        System.out.println("Async row count: " + rowCount + ", time: " + (endTime.get() - t1) + " ms");
    }

    static void insertVirtualThread(Statement stmt, int rowCount) throws Exception {
        long t1 = System.currentTimeMillis();
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < rowCount; i++) {
                executor.submit(() -> {
                    return stmt.executeUpdate("INSERT INTO test VALUES('a', 1, 10)");
                });
            }
        }
        long t2 = System.currentTimeMillis();
        System.out.println("VirtualThread row count: " + rowCount + ", time: " + (t2 - t1) + " ms");
    }

    static void insertVirtualThreadAsync(Statement stmt, int rowCount) throws Exception {
        JdbcStatement stmtAsync = (JdbcStatement) stmt;
        CountDownLatch latch = new CountDownLatch(rowCount);
        long t1 = System.currentTimeMillis();
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < rowCount; i++) {
                executor.submit(() -> {
                    stmtAsync.executeUpdateAsync("INSERT INTO test VALUES('a', 1, 10)")
                            .onComplete(ar -> {
                                if (ar.isFailed()) {
                                    ar.getCause().printStackTrace();
                                }
                                latch.countDown();
                            });
                    return null;
                });
            }
        }
        latch.await();
        long t2 = System.currentTimeMillis();
        System.out
                .println("Async VirtualThread row count: " + rowCount + ", time: " + (t2 - t1) + " ms");
    }

    static void init(Statement stmt) throws Exception {
        stmt.executeUpdate("DROP TABLE IF EXISTS test");
        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS test(name varchar(20), f1 int, f2 int)");
    }

    static void query(Statement stmt) throws Exception {
        stmt.executeUpdate("set QUERY_CACHE_SIZE 0");
        stmt.executeUpdate("set olap_threshold 1");
        stmt.executeUpdate("set olap_batch_size 128");
        // stmt.executeUpdate("set olap_batch_size 256");

        String sql = "SELECT count(*), sum(f1+f2) FROM test";

        for (int i = 0; i < 100; i++) {
            querySync(stmt, sql);
        }
    }

    static void querySync(Statement stmt, String sql) throws Exception {
        int count = 1;
        long t1 = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            stmt.executeQuery(sql).close();
        }
        long t2 = System.currentTimeMillis();
        System.out.println("time: " + ((t2 - t1) / count) + "ms");
        // System.out.println("time: " + ((t2 - t1) * 1000 / count) + "μs");
    }

    public static void main2(String[] args) {
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            IntStream.range(0, 10_000).forEach(i -> {
                executor.submit(() -> {
                    Thread.sleep(Duration.ofMillis(10));
                    return i;
                });
            });
        }
    }

    public static void testLock() throws Exception {

        // Object lock = new Object();
        final ReentrantLock rl = new ReentrantLock();
        ThreadFactory factory = Thread.ofVirtual().name("vt-", 1).factory();
        try (var executor = Executors.newThreadPerTaskExecutor(factory)) {
            executor.submit(() -> {
                int i = 0;
                // synchronized (lock) { //会挂起系统线程
                // i++;
                // }

                rl.lock(); // 不会挂起系统线程
                i++;
                rl.unlock();
                System.out.println("a: " + i);
            });

            executor.submit(() -> {
                int i = 0;
                // synchronized (lock) {
                // i++;
                // }

                rl.lock();
                i++;
                rl.unlock();
                System.out.println("b: " + i);
            });

            for (int i = 0; i < 50; i++) {
                final int i2 = i;
                executor.submit(() -> {
                    rl.lock();
                    System.out.println(i2);
                });
            }
        }
        System.out.println("main end");
    }
}
