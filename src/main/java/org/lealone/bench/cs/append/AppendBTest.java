/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.bench.cs.append;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.lealone.bench.cs.ClientServerBTest;
import org.lealone.client.jdbc.JdbcStatement;

public abstract class AppendBTest extends ClientServerBTest {

    private int loop = 100;
    private int count = 500;
    private int threadCount = 8;
    private int rowCount = loop * count * threadCount;
    private String[] sqls = new String[rowCount];
    boolean async;

    @Override
    public void run() throws Exception {
        init();
        run(threadCount);
    }

    public abstract Connection getConnection() throws Exception;

    public static Connection getConnection(int port) throws Exception {
        String url = "jdbc:postgresql://localhost:" + port + "/test";
        return getConnection(url, "test", "test");
    }

    private String getName() {
        return getClass().getSimpleName();
    }

    @Override
    protected void init() throws Exception {
        Connection conn = getConnection();
        Statement statement = conn.createStatement();
        statement.executeUpdate("drop table if exists AppendBTest");
        String sql = "create table if not exists AppendBTest(f1 int, f2 int)";
        statement.executeUpdate(sql);

        for (int i = 1; i <= rowCount; i++) {
            sqls[i - 1] = "insert into AppendBTest values(" + i + ",1)";
        }
        close(statement, conn);
    }

    @Override
    protected void run(int threadCount) throws Exception {
        UpdateThread[] threads = new UpdateThread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            Connection conn = getConnection();
            threads[i] = new UpdateThread(i, conn);
        }
        long t1 = System.nanoTime();
        for (int i = 0; i < threadCount; i++) {
            threads[i].start();
        }
        for (int i = 0; i < threadCount; i++) {
            threads[i].join();
        }
        long t2 = System.nanoTime();
        System.out.println(getName() + " total time: " + //
                TimeUnit.NANOSECONDS.toMillis(t2 - t1) + " ms");
    }

    private class UpdateThread extends Thread {
        Connection conn;
        Statement stmt;
        int start;

        UpdateThread(int id, Connection conn) throws Exception {
            super("UpdateThread-" + id);
            this.conn = conn;
            this.stmt = conn.createStatement();
            start = loop * count * id;
        }

        @Override
        public void run() {
            try {
                if (async)
                    executeUpdateAsync(stmt, start);
                else
                    executeUpdate(stmt, start);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                close(stmt, conn);
            }
        }
    }

    private void executeUpdateAsync(Statement statement, int start) throws Exception {
        JdbcStatement stmt = (JdbcStatement) statement;
        for (int j = 0; j < loop; j++) {
            CountDownLatch latch = new CountDownLatch(count);
            long t1 = System.nanoTime();
            for (int i = 0; i < count; i++) {
                stmt.executeUpdateAsync(sqls[start++]).onComplete(ar -> {
                    latch.countDown();
                });
            }
            latch.await();
            long t2 = System.nanoTime();
            System.out.println(getName() + ": " + TimeUnit.NANOSECONDS.toMicros(t2 - t1) / count);
        }
        System.out.println();
        System.out.println("time: 微秒");
        System.out.println("loop: " + loop + " * " + count);
    }

    private void executeUpdate(Statement statement, int start) throws Exception {
        for (int j = 0; j < loop; j++) {
            long t1 = System.nanoTime();
            for (int i = 0; i < count; i++)
                statement.executeUpdate(sqls[start++]);
            long t2 = System.nanoTime();
            System.out.println(getName() + ": " + TimeUnit.NANOSECONDS.toMicros(t2 - t1) / count);
        }
        System.out.println();
        System.out.println("time: 微秒");
        System.out.println("loop: " + loop + " * " + count);
    }

    private static void close(Statement stmt, Connection conn) {
        try {
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
