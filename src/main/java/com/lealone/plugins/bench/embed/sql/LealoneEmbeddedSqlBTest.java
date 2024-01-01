/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.bench.embed.sql;

import java.sql.Connection;
import java.sql.Statement;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.lealone.client.jdbc.JdbcStatement;
import com.lealone.db.SysProperties;
import com.lealone.db.scheduler.EmbeddedScheduler;
import com.lealone.db.scheduler.Scheduler;
import com.lealone.db.scheduler.SchedulerFactory;
import com.lealone.storage.page.PageOperation;
import com.lealone.test.aote.TransactionEngineTest;
import com.lealone.transaction.TransactionEngine;

public class LealoneEmbeddedSqlBTest extends EmbeddedSqlBenchTest {

    public static void main(String[] args) throws Exception {
        new LealoneEmbeddedSqlBTest().run();
    }

    private final HashMap<String, String> config = new HashMap<>();
    private final AtomicInteger index = new AtomicInteger(0);
    private Scheduler[] schedulers;
    private TransactionEngine te;

    @Override
    protected void resetFields() {
        super.resetFields();
        index.set(0);
    }

    @Override
    protected void init() throws Exception {
        if (!inited.compareAndSet(false, true))
            return;
        SysProperties.setBaseDir(BENCH_TEST_BASE_DIR);

        String factoryType = "RoundRobin";
        factoryType = "Random";
        // factoryType = "LoadBalance";
        config.put("page_operation_handler_factory_type", factoryType);
        // config.put("page_operation_handler_count", (threadCount + 1) + "");
        createPageOperationHandlers();

        initTransactionEngineConfig(config);
        te = TransactionEngineTest.getTransactionEngine(config);

        super.init();
    }

    @Override
    protected void destroy() throws Exception {
        te.close();
    }

    private void createPageOperationHandlers() {
        schedulers = new Scheduler[threadCount];
        HashMap<String, String> config = new HashMap<>();
        for (int i = 0; i < threadCount; i++) {
            schedulers[i] = new EmbeddedScheduler(i, threadCount, config);
            schedulers[i].start();
        }
        SchedulerFactory.create(null, schedulers);
    }

    @Override
    protected Connection getConnection() throws Exception {
        return getEmbeddedLealoneConnection();
    }

    @Override
    protected void update(Statement stmt, int start, int end) throws Exception {
        JdbcStatement statement = (JdbcStatement) stmt;
        for (int i = start; i < end; i++) {
            Integer key;
            if (isRandom())
                key = randomKeys[i];
            else
                key = i;
            // String value = "value-";// "value-" + key;
            // map.put(key, value);

            String sql = "update SqlBenchTest set f2 = 'value2' where f1 =" + key;
            statement.executeUpdate(sql);
            notifyOperationComplete();
            // System.out.println(getName() + " key:" + key);
            // AsyncHandler<AsyncResult<Integer>> handler = ar -> {
            // // if (count.decrementAndGet() <= 0) {
            // //
            // // endTime.set(System.currentTimeMillis());
            // // latch.countDown();
            // // }
            // notifyOperationComplete();
            // };
            // statement.executeUpdateAsync(sql, handler);
        }
    }

    @Override
    protected LealoneEmbeddedSqlBenchTestTask createBenchTestTask(int start, int end) throws Exception {
        return new LealoneEmbeddedSqlBenchTestTask(start, end);
    }

    class LealoneEmbeddedSqlBenchTestTask extends SqlBenchTestTask implements PageOperation {

        LealoneEmbeddedSqlBenchTestTask(int start, int end) throws Exception {
            super(start, end);
            Scheduler h = schedulers[index.getAndIncrement()];
            h.handlePageOperation(this);
        }

        @Override
        public boolean needCreateThread() {
            return false;
        }
    }
}
