/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.bench.cs.index;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

import com.lealone.plugins.bench.DbType;
import com.lealone.plugins.bench.cs.write.ClientServerWriteBTest;

public class IndexCreateBTest extends ClientServerWriteBTest {

    protected IndexCreateBTest() {
        benchTestLoop = 20;
        rowCount = 10 * 10000;
    }

    @Override
    public void run() throws Exception {
        for (int i = 0; i < benchTestLoop; i++) {
            run0();
        }
    }

    private void run0() throws Exception {
        Connection conn = getConnection();
        Statement statement = conn.createStatement();
        statement.executeUpdate("drop table if exists IndexCreateBTest");
        String sql = "create table if not exists IndexCreateBTest"
                + "(pk int primary key, f1 int, f2 varchar(30))";
        statement.executeUpdate(sql);

        batchPreparedInsert(conn);
        // batchInsert(statement);
        // createIndex(statement);
        // batchPreparedUpdate(conn);
        close(statement, conn);
    }

    public void batchInsert(Statement statement) throws Exception {
        long t1 = System.nanoTime();
        for (int i = 1; i <= rowCount; i++) {
            StringBuilder sql = new StringBuilder("INSERT INTO IndexCreateBTest(pk, f1, f2) VALUES(");
            sql.append(i).append(',');
            sql.append(i * 10).append(',');
            sql.append("'v-").append(i * 10).append("')");
            statement.addBatch(sql.toString());
            if (i % 1000 == 0)
                statement.executeBatch();
        }
        long t2 = System.nanoTime();
        System.out.println(getBTestName() + " batchInsert row count: " + rowCount + " total time: "
                + TimeUnit.NANOSECONDS.toMillis(t2 - t1) + " ms");
    }

    public void batchPreparedInsert(Connection conn) throws Exception {
        long t1 = System.nanoTime();
        String sql = "INSERT INTO IndexCreateBTest(pk, f1, f2) VALUES(?,?,?)";
        PreparedStatement ps = conn.prepareStatement(sql);
        for (int i = 1; i <= rowCount; i++) {
            ps.setInt(1, i);
            ps.setInt(2, i * 10);
            ps.setString(3, "v-" + (i * 10));
            ps.addBatch();
            if (i % 1000 == 0)
                ps.executeBatch();
        }
        ps.close();
        long t2 = System.nanoTime();
        System.out.println(getBTestName() + " batchPreparedInsert row count: " + rowCount
                + " total time: " + TimeUnit.NANOSECONDS.toMillis(t2 - t1) + " ms");
    }

    public void batchPreparedUpdate(Connection conn) throws Exception {
        long t1 = System.nanoTime();
        String sql = "UPDATE IndexCreateBTest set f1=?, f2=? where pk=?";
        PreparedStatement ps = conn.prepareStatement(sql);
        for (int i = 1; i <= rowCount; i++) {
            ps.setInt(1, i * 100);
            ps.setString(2, "v-" + (i * 100));
            ps.setInt(3, i);
            ps.addBatch();
            if (i % 1000 == 0)
                ps.executeBatch();
        }
        ps.close();
        long t2 = System.nanoTime();
        System.out.println(getBTestName() + " batchPreparedUpdate row count: " + rowCount
                + " total time: " + TimeUnit.NANOSECONDS.toMillis(t2 - t1) + " ms");
    }

    public void createIndex(Statement statement) throws Exception {
        long t1 = System.nanoTime();
        String dsql = "DROP INDEX IF EXISTS IndexCreateBTest_idx1";
        String csql = "CREATE INDEX IF NOT EXISTS IndexCreateBTest_idx1 ON IndexCreateBTest(f2)";
        if (dbType == DbType.MYSQL) {
            dsql = "DROP INDEX IndexCreateBTest_idx1 on IndexCreateBTest";
            csql = "CREATE INDEX IndexCreateBTest_idx1 ON IndexCreateBTest(f2)";
        }
        try {
            statement.executeUpdate(dsql);
        } catch (Exception e) {
        }
        try {
            statement.executeUpdate(csql);
        } catch (Exception e) {
        }
        long t2 = System.nanoTime();
        System.out.println(getBTestName() + " createIndex row count: " + rowCount + " total time: "
                + TimeUnit.NANOSECONDS.toMillis(t2 - t1) + " ms");
    }
}
