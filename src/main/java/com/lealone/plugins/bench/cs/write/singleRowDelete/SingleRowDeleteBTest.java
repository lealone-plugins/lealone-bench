/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.bench.cs.write.singleRowDelete;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;

import com.lealone.plugins.bench.cs.write.ClientServerWriteBTest;

public abstract class SingleRowDeleteBTest extends ClientServerWriteBTest {

    protected SingleRowDeleteBTest() {
        benchTestLoop = 20;
        outerLoop = 15;
        threadCount = 16;
        sqlCountPerInnerLoop = 10;
        innerLoop = 20;
        // prepare = true;
        // reinit = false;
        // autoCommit = false;
        // embedded = true;

        rowCount = 30000;
    }

    @Override
    protected void init() throws Exception {
        Connection conn = getConnection();
        Statement statement = conn.createStatement();
        statement.executeUpdate("drop table if exists SingleRowDeleteBTest");
        String sql = "create table if not exists SingleRowDeleteBTest(pk int primary key, f1 int)";
        statement.executeUpdate(sql);

        sql = "insert into SingleRowDeleteBTest values(?,1)";
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
    protected UpdateThreadBase createBTestThread(int id, Connection conn) {
        return new UpdateThread(id, conn);
    }

    private class UpdateThread extends UpdateThreadBase {

        UpdateThread(int id, Connection conn) {
            super(id, conn);
            prepareStatement("delete from SingleRowDeleteBTest where pk=?");
        }

        @Override
        protected String nextSql() {
            int pk = random.nextInt(rowCount);
            return "delete from SingleRowDeleteBTest where pk=" + pk;
        }

        @Override
        protected void prepare() throws Exception {
            int pk = random.nextInt(rowCount);
            ps.setInt(1, pk);
        }
    }
}
