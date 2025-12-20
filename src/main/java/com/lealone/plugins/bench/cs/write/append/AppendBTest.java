/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.bench.cs.write.append;

import java.sql.Connection;
import java.sql.Statement;

import com.lealone.plugins.bench.cs.write.ClientServerWriteBTest;

public abstract class AppendBTest extends ClientServerWriteBTest {

    public AppendBTest() {
    }

    @Override
    protected void init() throws Exception {
        Connection conn = getConnection();
        Statement statement = conn.createStatement();
        // statement.executeUpdate("TRUNCATE table AppendBTest");
        statement.executeUpdate("drop table if exists AppendBTest");
        String sql = "create table if not exists AppendBTest(name varchar(20), f1 int, f2 bigint)";
        statement.executeUpdate(sql);
        // for (int i = 0; i < 0; i++) {
        // statement.executeUpdate("drop table if exists AppendBTest" + i);
        // sql = "create table if not exists AppendBTest" + i + "(name varchar(20), f1 int, f2 bigint)";
        // statement.executeUpdate(sql);
        // }
        close(statement, conn);
    }

    @Override
    protected UpdateThreadBase createBTestThread(int id, Connection conn) {
        return new UpdateThread(id, conn);
    }

    private class UpdateThread extends UpdateThreadBase {

        UpdateThread(int id, Connection conn) {
            super(id, conn);
            prepareStatement("insert into AppendBTest values(?,?,?)");
        }

        @Override
        protected String nextSql() {
            int i = id.incrementAndGet();
            // int t = i % 4;
            // return "insert into AppendBTest" + t + " values('n" + i + "'," + i + "," + (i * 10) + ")";

            return "insert into AppendBTest values('n" + i + "'," + i + "," + (i * 10) + ")";
        }

        @Override
        protected void prepare() throws Exception {
            int i = id.incrementAndGet();
            ps.setString(1, "n" + i);
            ps.setInt(2, i);
            ps.setLong(3, i * 10);
        }
    }
}
