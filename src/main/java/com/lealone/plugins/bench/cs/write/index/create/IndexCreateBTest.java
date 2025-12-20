/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.bench.cs.write.index.create;

import java.sql.Connection;
import java.sql.Statement;

import com.lealone.plugins.bench.cs.write.singleThreadBatch.BatchBTest;

public class IndexCreateBTest extends BatchBTest {

    private Connection conn;
    private Statement statement;

    protected IndexCreateBTest() {
        benchTestLoop = 10;
        tableName = "IndexCreateBTest";
    }

    @Override
    public void run() throws Exception {
        conn = getConnection();
        statement = conn.createStatement();
        init0();
        for (int i = 0; i < benchTestLoop; i++) {
            run0();
        }
        close(statement, conn);
    }

    private void init0() throws Exception {
        batchPreparedInsert(conn, statement);
    }

    private void run0() throws Exception {
        // createIndex(statement, "f1");
        createIndex(statement, "f2");
        // createIndex(statement, "f2,f1");
    }
}
