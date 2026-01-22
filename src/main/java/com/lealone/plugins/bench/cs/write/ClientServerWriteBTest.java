/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.bench.cs.write;

import java.sql.Statement;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.lealone.client.jdbc.JdbcPreparedStatement;
import com.lealone.client.jdbc.JdbcStatement;
import com.lealone.plugins.bench.DbType;
import com.lealone.plugins.bench.cs.ClientServerBTest;

public abstract class ClientServerWriteBTest extends ClientServerBTest {

    protected abstract class UpdateThreadBase extends ClientServerBTestThread {

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
            JdbcStatement stmt = (JdbcStatement) statement;
            AtomicInteger counter = new AtomicInteger(sqlCountPerInnerLoop * innerLoop);
            for (int i = 0; i < innerLoop; i++) {
                for (int j = 0; j < sqlCountPerInnerLoop; j++) {
                    stmt.executeUpdateAsync(nextSql()).onComplete(ar -> {
                        if (counter.decrementAndGet() == 0) {
                            onComplete();
                        }
                    });
                }
            }
        }

        protected void executePreparedUpdateAsync() throws Exception {
            JdbcPreparedStatement ps = (JdbcPreparedStatement) this.ps;
            AtomicInteger counter = new AtomicInteger(sqlCountPerInnerLoop * innerLoop);
            for (int i = 0; i < innerLoop; i++) {
                for (int j = 0; j < sqlCountPerInnerLoop; j++) {
                    prepare();
                    ps.executeUpdateAsync().onComplete(ar -> {
                        if (counter.decrementAndGet() == 0) {
                            onComplete();
                        }
                    });
                }
            }
        }

        protected void executeUpdate(Statement statement) throws Exception {
            for (int i = 0; i < innerLoop; i++) {
                if (useVirtualThread) {
                    for (int j = 0; j < sqlCountPerInnerLoop; j++) {
                        executorService.submit(() -> {
                            // 其他数据库用这种方式更慢，只适合用stmtQueue
                            if (dbType == DbType.LEALONE) {
                                Statement s = statement;
                                // s = nextStatement();
                                s.executeUpdate(nextSql());
                            } else {
                                Statement s = stmtQueue.poll(1, TimeUnit.HOURS);
                                s.executeUpdate(nextSql());
                                stmtQueue.add(s);
                            }
                            onComplete(1);
                            return 1;
                        });
                    }
                } else {
                    for (int j = 0; j < sqlCountPerInnerLoop; j++) {
                        statement.executeUpdate(nextSql());
                    }
                }
            }
        }

        protected void executePreparedUpdate() throws Exception {
            for (int i = 0; i < innerLoop; i++) {
                for (int j = 0; j < sqlCountPerInnerLoop; j++) {
                    prepare();
                    ps.executeUpdate();
                }
            }
        }

        protected void executeBatchUpdate() throws Exception {
            for (int i = 0; i < innerLoop; i++) {
                for (int j = 0; j < sqlCountPerInnerLoop; j++) {
                    stmt.addBatch(nextSql());
                }
                stmt.executeBatch();
            }
        }

        protected void executePreparedBatchUpdate() throws Exception {
            for (int i = 0; i < innerLoop; i++) {
                for (int j = 0; j < sqlCountPerInnerLoop; j++) {
                    prepare();
                    ps.addBatch();
                }
                ps.executeBatch();
            }
        }
    }
}
