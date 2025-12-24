/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.bench.cs.query;

import java.sql.Connection;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicInteger;

import com.lealone.client.jdbc.JdbcPreparedStatement;
import com.lealone.client.jdbc.JdbcStatement;
import com.lealone.plugins.bench.cs.ClientServerBTest;

public abstract class ClientServerQueryBTest extends ClientServerBTest {

    public ClientServerQueryBTest() {
        reinit = false;
    }

    protected abstract class QueryThreadBase extends ClientServerBTestThread {

        public QueryThreadBase(int id, Connection conn) {
            super(id, conn);
        }

        @Override
        protected void execute() throws Exception {
            if (async) {
                if (prepare)
                    executePreparedQueryAsync();
                else
                    executeQueryAsync(stmt);
            } else {
                if (prepare)
                    executePreparedQuery();
                else
                    executeQuery(stmt);
            }
        }

        protected void executeQueryAsync(Statement statement) throws Exception {
            JdbcStatement stmt = (JdbcStatement) statement;
            AtomicInteger counter = new AtomicInteger(sqlCountPerInnerLoop * innerLoop);
            long t1 = System.nanoTime();
            for (int j = 0; j < innerLoop; j++) {
                for (int i = 0; i < sqlCountPerInnerLoop; i++) {
                    stmt.executeQueryAsync(nextSql()).onComplete(ar -> {
                        if (counter.decrementAndGet() == 0) {
                            printInnerLoopResult(t1);
                            onComplete();
                        }
                    });
                }
            }
        }

        protected void executePreparedQueryAsync() throws Exception {
            JdbcPreparedStatement ps = (JdbcPreparedStatement) this.ps;
            AtomicInteger counter = new AtomicInteger(sqlCountPerInnerLoop * innerLoop);
            long t1 = System.nanoTime();
            for (int j = 0; j < innerLoop; j++) {
                for (int i = 0; i < sqlCountPerInnerLoop; i++) {
                    prepare();
                    ps.executeQueryAsync().onComplete(ar -> {
                        if (counter.decrementAndGet() == 0) {
                            printInnerLoopResult(t1);
                            onComplete();
                        }
                    });
                }
            }
        }

        protected void executeQuery(Statement statement) throws Exception {
            long t1 = System.nanoTime();
            for (int j = 0; j < innerLoop; j++) {
                for (int i = 0; i < sqlCountPerInnerLoop; i++) {
                    statement.executeQuery(nextSql());
                }
            }
            printInnerLoopResult(t1);
        }

        protected void executePreparedQuery() throws Exception {
            long t1 = System.nanoTime();
            for (int j = 0; j < innerLoop; j++) {
                for (int i = 0; i < sqlCountPerInnerLoop; i++) {
                    prepare();
                    ps.executeQuery();
                }
            }
            printInnerLoopResult(t1);
        }
    }
}
