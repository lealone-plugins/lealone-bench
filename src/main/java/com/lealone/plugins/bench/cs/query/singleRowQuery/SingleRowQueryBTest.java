/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.bench.cs.query.singleRowQuery;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;

import com.lealone.plugins.bench.cs.query.ClientServerQueryBTest;

public abstract class SingleRowQueryBTest extends ClientServerQueryBTest {

    public SingleRowQueryBTest() {
        benchTestLoop = 20;
        outerLoop = 15;
        threadCount = 16;
        sqlCountPerInnerLoop = 10;
        innerLoop = 20;
        rowCount = 10000;
        // prepare = true;
        // embedded = true;
    }

    @Override
    protected void init() throws Exception {
        Connection conn = getConnection();
        Statement statement = conn.createStatement();
        statement.executeUpdate("drop table if exists SingleRowQueryBTest");
        String sql = "create table if not exists SingleRowQueryBTest(pk int primary key, f1 int)";
        statement.executeUpdate(sql);

        sql = "insert into SingleRowQueryBTest values(?,1)";
        PreparedStatement ps = conn.prepareStatement(sql);

        for (int row = 1; row <= rowCount; row++) {
            ps.setInt(1, row);
            ps.addBatch();
            if (row % 100 == 0 || row == rowCount) {
                ps.executeBatch();
                ps.clearBatch();
            }
        }
        close(statement, ps, conn);
    }

    @Override
    protected QueryThreadBase createBTestThread(int id, Connection conn) {
        return new QueryThread(id, conn);
    }

    private class QueryThread extends QueryThreadBase {

        QueryThread(int id, Connection conn) {
            super(id, conn);
            prepareStatement("select * from SingleRowQueryBTest where pk=?");
        }

        @Override
        protected String nextSql() {
            return "select * from SingleRowQueryBTest where pk=" + random.nextInt(rowCount);
        }

        @Override
        protected void prepare() throws Exception {
            ps.setInt(1, random.nextInt(rowCount));
        }
    }
}
