/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.bench.cs.write.insert;

import java.sql.Connection;
import java.sql.Statement;

import com.lealone.plugins.bench.cs.write.ClientServerWriteBTest;

public abstract class InsertBTest extends ClientServerWriteBTest {

    protected InsertBTest() {
    }

    @Override
    protected void init() throws Exception {
        Connection conn = getConnection();
        Statement statement = conn.createStatement();
        statement.executeUpdate("drop table if exists InsertBTest");
        String sql = "create table if not exists InsertBTest(pk int auto_increment primary key, f1 int)"
                + " parameters(page_size='8k')";
        sql = "create table if not exists InsertBTest(pk int primary key, f1 int)";
        statement.executeUpdate(sql);
        close(statement, conn);
    }

    @Override
    protected UpdateThreadBase createBTestThread(int id, Connection conn) {
        return new UpdateThread(id, conn);
    }

    private class UpdateThread extends UpdateThreadBase {

        UpdateThread(int id, Connection conn) {
            super(id, conn);
            prepareStatement("insert into InsertBTest values(?,1)");
        }

        @Override
        protected String nextSql() {
            return "insert into InsertBTest values(" + id.incrementAndGet() + ",1)";
        }

        @Override
        protected void prepare() throws Exception {
            ps.setInt(1, id.incrementAndGet());
        }
    }
}
