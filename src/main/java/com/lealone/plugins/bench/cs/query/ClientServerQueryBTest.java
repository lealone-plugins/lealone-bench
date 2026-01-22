/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.bench.cs.query;

import java.sql.Statement;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.lealone.client.jdbc.JdbcPreparedStatement;
import com.lealone.client.jdbc.JdbcStatement;
import com.lealone.plugins.bench.DbType;
import com.lealone.plugins.bench.cs.ClientServerBTest;

public abstract class ClientServerQueryBTest extends ClientServerBTest {

    public ClientServerQueryBTest() {
        reinit = false;
    }

    @Override
    protected boolean isQuery() {
        return true;
    }

    protected abstract class QueryThreadBase extends ClientServerBTestThread {

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
            for (int i = 0; i < innerLoop; i++) {
                for (int j = 0; j < sqlCountPerInnerLoop; j++) {
                    stmt.executeQueryAsync(nextSql()).onComplete(ar -> {
                        if (counter.decrementAndGet() == 0) {
                            onComplete();
                        }
                    });
                }
            }
        }

        protected void executePreparedQueryAsync() throws Exception {
            JdbcPreparedStatement ps = (JdbcPreparedStatement) this.ps;
            AtomicInteger counter = new AtomicInteger(sqlCountPerInnerLoop * innerLoop);
            for (int i = 0; i < innerLoop; i++) {
                for (int j = 0; j < sqlCountPerInnerLoop; j++) {
                    prepare();
                    ps.executeQueryAsync().onComplete(ar -> {
                        if (counter.decrementAndGet() == 0) {
                            onComplete();
                        }
                    });
                }
            }
        }

        protected void executeQuery(Statement statement) throws Exception {
            for (int i = 0; i < innerLoop; i++) {
                for (int j = 0; j < sqlCountPerInnerLoop; j++) {
                    if (useVirtualThread) {
                        executorService.submit(() -> {
                            if (dbType == DbType.LEALONE) {
                                try {
                                    Statement s = nextStatement();
                                    s = statement; // 这种方式性能更好，调度器需要执行写操作的次数更少
                                    s.executeQuery(nextSql());
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            } else {
                                Statement s = stmtQueue.poll(1, TimeUnit.HOURS);
                                s.executeQuery(nextSql());
                                stmtQueue.add(s);
                            }
                            onComplete(1);
                            return 1;
                        });
                    } else {
                        statement.executeQuery(nextSql());
                    }
                }
            }
        }

        protected void executePreparedQuery() throws Exception {
            for (int i = 0; i < innerLoop; i++) {
                for (int j = 0; j < sqlCountPerInnerLoop; j++) {
                    if (useVirtualThread) {
                        executorService.submit(() -> {
                            prepare();
                            ps.executeQuery();
                            onComplete(1);
                            return 1;
                        });
                    } else {
                        prepare();
                        ps.executeQuery();
                    }
                }
            }
        }
    }
}
