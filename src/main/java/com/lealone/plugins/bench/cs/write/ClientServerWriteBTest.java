/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.bench.cs.write;

import java.sql.Connection;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicInteger;

import com.lealone.client.jdbc.JdbcPreparedStatement;
import com.lealone.client.jdbc.JdbcStatement;
import com.lealone.plugins.bench.cs.ClientServerBTest;

public abstract class ClientServerWriteBTest extends ClientServerBTest {

    protected abstract class UpdateThreadBase extends ClientServerBTestThread {

        public UpdateThreadBase(int id, Connection conn) {
            super(id, conn);
        }

        @Override
        protected void execute() throws Exception {
            if (batch) {
                if (prepare)
                    executePreparedBatchUpdate();
                else
                    executeBatchUpdate();
            } else {
                if (async) {
                    if (prepare)
                        executePreparedUpdateAsync();
                    else
                        executeUpdateAsync(stmt);
                } else {
                    if (prepare)
                        executePreparedUpdate();
                    else
                        executeUpdate(stmt);
                }
            }
        }

        protected void executeUpdateAsync(Statement statement) throws Exception {
            long t1 = System.nanoTime();
            JdbcStatement stmt = (JdbcStatement) statement;
            AtomicInteger counter = new AtomicInteger(sqlCountPerInnerLoop * innerLoop);
            for (int j = 0; j < innerLoop; j++) {
                for (int i = 0; i < sqlCountPerInnerLoop; i++) {
                    stmt.executeUpdateAsync(nextSql()).onComplete(ar -> {
                        if (counter.decrementAndGet() == 0) {
                            printInnerLoopResult(t1);
                            onComplete();
                        }
                    });
                }
            }
        }

        protected void executePreparedUpdateAsync() throws Exception {
            long t1 = System.nanoTime();
            JdbcPreparedStatement ps = (JdbcPreparedStatement) this.ps;
            AtomicInteger counter = new AtomicInteger(sqlCountPerInnerLoop * innerLoop);
            for (int j = 0; j < innerLoop; j++) {
                for (int i = 0; i < sqlCountPerInnerLoop; i++) {
                    prepare();
                    ps.executeUpdateAsync().onComplete(ar -> {
                        if (counter.decrementAndGet() == 0) {
                            printInnerLoopResult(t1);
                            onComplete();
                        }
                    });
                }
            }
        }

        protected void executeUpdate(Statement statement) throws Exception {
            long t1 = System.nanoTime();
            for (int j = 0; j < innerLoop; j++) {
                for (int i = 0; i < sqlCountPerInnerLoop; i++) {
                    statement.executeUpdate(nextSql());
                }
            }
            printInnerLoopResult(t1);
        }

        protected void executePreparedUpdate() throws Exception {
            long t1 = System.nanoTime();
            for (int j = 0; j < innerLoop; j++) {
                for (int i = 0; i < sqlCountPerInnerLoop; i++) {
                    prepare();
                    ps.executeUpdate();
                }
            }
            printInnerLoopResult(t1);
        }

        protected void executeBatchUpdate() throws Exception {
            long t1 = System.nanoTime();
            for (int j = 0; j < innerLoop; j++) {
                for (int i = 0; i < sqlCountPerInnerLoop; i++) {
                    stmt.addBatch(nextSql());
                }
                stmt.executeBatch();
            }
            printInnerLoopResult(t1);
        }

        protected void executePreparedBatchUpdate() throws Exception {
            long t1 = System.nanoTime();
            for (int j = 0; j < innerLoop; j++) {
                for (int i = 0; i < sqlCountPerInnerLoop; i++) {
                    prepare();
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            printInnerLoopResult(t1);
        }
    }
}
