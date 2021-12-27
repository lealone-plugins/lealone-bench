/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.bench.cs.columnlock;

import java.sql.Connection;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.lealone.bench.cs.ClientServerBTest;
import org.lealone.client.jdbc.JdbcStatement;

public abstract class ColumnLockBTest extends ClientServerBTest {

    private int loop = 100;
    private int count = 500;
    private int columnCount = 16;
    private String[] sqls = new String[columnCount];
    private String[] sqlsWarmUp = new String[columnCount];
    boolean async;

    @Override
    public void run() throws Exception {
        init();
        run(columnCount);
    }

    @Override
    protected void init() throws Exception {
        Connection conn = getConnection();
        Statement statement = conn.createStatement();
        statement.executeUpdate("drop table if exists ColumnLockPerfTest");

        StringBuilder buff = new StringBuilder();
        buff.append("create table if not exists ColumnLockPerfTest(pk int primary key");
        for (int i = 1; i <= columnCount; i++) {
            buff.append(",f").append(i).append(" int");
        }
        buff.append(")");
        statement.executeUpdate(buff.toString());

        for (int row = 1; row <= 9; row++) {
            buff = new StringBuilder();
            buff.append("insert into ColumnLockPerfTest values(").append(row);
            for (int i = 1; i <= columnCount; i++) {
                buff.append(",").append(i * 10);
            }
            buff.append(")");
            statement.executeUpdate(buff.toString());
        }
        for (int i = 1; i <= columnCount; i++) {
            buff = new StringBuilder();
            buff.append("update ColumnLockPerfTest set f").append(i).append(" = ").append(i * 1000)
                    .append(" where pk=5");
            sqls[i - 1] = buff.toString();
        }
        for (int i = 1; i <= columnCount; i++) {
            buff = new StringBuilder();
            buff.append("update ColumnLockPerfTest set f").append(i).append(" = ").append(i * 100)
                    .append(" where pk=5");
            sqlsWarmUp[i - 1] = buff.toString();
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
