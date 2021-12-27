/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.bench.cs.multiRowsUpdate;

import java.sql.Connection;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.lealone.bench.cs.ClientServerBTest;
import org.lealone.client.jdbc.JdbcStatement;

public abstract class MultiRowsUpdateBTest extends ClientServerBTest {

    private int loop = 100;
    private int count = 500;
    private int rowCount = 8;
    private String[] sqls = new String[rowCount];
    private String[] sqlsWarmUp = new String[rowCount];
    boolean async;

    @Override
    public void run() throws Exception {
        init();
        run(rowCount);
    }

    @Override
    protected void init() throws Exception {
        Connection conn = getConnection();
        Statement statement = conn.createStatement();
        statement.executeUpdate("drop table if exists MultiRowsUpdateBTest");
        String sql = "create table if not exists MultiRowsUpdateBTest(pk int primary key, f1 int)";
        statement.executeUpdate(sql);

        for (int row = 1; row <= rowCount; row++) {
            sql = "insert into MultiRowsUpdateBTest values(" + row + ",1)";
            statement.executeUpdate(sql);
        }
        for (int i = 1; i <= rowCount; i++) {
            sqls[i - 1] = "update MultiRowsUpdateBTest set f1=10 where pk=" + i;
        }
        for (int i = 1; i <= rowCount; i++) {
            sqlsWarmUp[i - 1] = "update MultiRowsUpdateBTest set f1=20 where pk=" + i;
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
        for (int i = 0; i < threadCount; i++) {
            threads[i].warmUp();
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
        String sql;
        String sqlWarmUp;

        UpdateThread(int id, Connection conn) throws Exception {
            super("UpdateThread-" + id);
            this.conn = conn;
            this.stmt = conn.createStatement();
            this.sql = sqls[id];
            this.sqlWarmUp = sqlsWarmUp[id];
        }

        @Override
        public void run() {
            try {
                if (async)
                    executeUpdateAsync(stmt, sql);
                else
                    executeUpdate(stmt, sql);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                close(stmt, conn);
            }
        }

        public void warmUp() throws Exception {
            for (int i = 0; i < count * 2; i++)
                stmt.executeUpdate(sqlWarmUp);
        }
    }

    private void executeUpdateAsync(Statement statement, String sql) throws Exception {
        JdbcStatement stmt = (JdbcStatement) statement;
        for (int j = 0; j < loop; j++) {
            CountDownLatch latch = new CountDownLatch(count);
            long t1 = System.nanoTime();
            for (int i = 0; i < count; i++) {
                stmt.executeUpdateAsync(sql).onComplete(ar -> {
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
        System.out.println("sql : " + sql);
    }

    private void executeUpdate(Statement statement, String sql) throws Exception {
        for (int j = 0; j < loop; j++) {
            long t1 = System.nanoTime();
            for (int i = 0; i < count; i++)
                statement.executeUpdate(sql);
            long t2 = System.nanoTime();
            System.out.println(getName() + ": " + TimeUnit.NANOSECONDS.toMicros(t2 - t1) / count);
        }
        System.out.println();
        System.out.println("time: 微秒");
        System.out.println("loop: " + loop + " * " + count);
        System.out.println("sql : " + sql);
    }
}
