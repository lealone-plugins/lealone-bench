/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.bench.cs.write.batchInsert;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.lealone.plugins.bench.cs.write.ClientServerWriteBTest;

public abstract class BatchInsertBTest extends ClientServerWriteBTest {

    protected boolean useRandom;

    protected BatchInsertBTest() {
        batch = true;
        // prepare = true;
        // useRandom = true;

        if (useRandom) {
            int size = benchTestLoop * outerLoop * threadCount * sqlCountPerInnerLoop * innerLoop;
            ids = new Integer[size];
            for (int i = 0; i < size; i++) {
                ids[i] = i + 1;
            }
            List<Integer> list = Arrays.asList(ids);
            Collections.shuffle(list);
            list.toArray(ids);
        }
    }

    Integer[] ids;

    @Override
    protected void init() throws Exception {
        Connection conn = getConnection();
        Statement statement = conn.createStatement();
        statement.executeUpdate("drop table if exists BatchInsertBTest");
        String sql = "create table if not exists BatchInsertBTest(pk int primary key, f1 int)";
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
            prepareStatement("insert into BatchInsertBTest values(?,1)");
        }

        @Override
        protected String nextSql() {
            return "insert into BatchInsertBTest values(" + id.incrementAndGet() + ",1)";
        }

        @Override
        protected void prepare() throws Exception {
            int v = id.incrementAndGet();
            if (useRandom)
                v = ids[v - 1];
            ps.setInt(1, v);
        }
    }
}
