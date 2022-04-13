/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.bench.embed.storage;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.lealone.db.value.ValueInt;
import org.lealone.db.value.ValueString;
import org.lealone.storage.aose.btree.BTreeMap;
import org.lealone.storage.aose.btree.page.PageOperations.Put;
import org.lealone.storage.page.DefaultPageOperationHandler;
import org.lealone.storage.page.PageOperation;
import org.lealone.storage.page.PageOperationHandler;
import org.lealone.storage.page.PageOperationHandlerFactory;

// -Xms512M -Xmx512M -XX:+PrintGCDetails -XX:+PrintGCTimeStamps
public class BTreeAsyncBTest extends StorageMapBTest {

    public static void main(String[] args) throws Exception {
        new BTreeAsyncBTest().run();
    }

    private final AtomicInteger lockCount = new AtomicInteger(0);
    private BTreeMap<Integer, String> btreeMap;

    @Override
    protected void testWrite(int loop) {
        multiThreadsRandomWriteAsync(loop);
        multiThreadsSerialWriteAsync(loop);
    }

    @Override
    protected void testRead(int loop) {
        multiThreadsRandomRead(loop);
        multiThreadsSerialRead(loop);
    }

    @Override
    protected void testConflict(int loop) {
        testConflict(loop, true);
    }

    @Override
    protected void beforeRun() {
        createPageOperationHandlers();
        super.beforeRun();
    }

    @Override
    protected void init() {
        if (!inited.compareAndSet(false, true))
            return;
        initConfig();
        createPageOperationHandlers();
        openStorage();
        map = btreeMap = storage.openBTreeMap(BTreeAsyncBTest.class.getSimpleName(), ValueInt.type, ValueString.type,
                null);
    }

    @Override
    protected void createData() {
        if (map.isEmpty()) {
            int t = threadCount;
            threadCount = 1;
            createPageOperationHandlers();
            threadCount = t;
            singleThreadSerialWrite();// 先生成初始数据
        }
    }

    @Override
    void singleThreadSerialWrite() {
        CountDownLatch latch = new CountDownLatch(rowCount);
        long t1 = System.currentTimeMillis();
        for (int i = 0; i < rowCount; i++) {
            map.put(i, "valueaaa", ar -> {
                latch.countDown();
            });
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        long t2 = System.currentTimeMillis();
        printResult("single-thread serial write time: " + (t2 - t1) + " ms, count: " + rowCount);
    }

    private void createPageOperationHandlers() {
        DefaultPageOperationHandler[] handlers = new DefaultPageOperationHandler[threadCount];
        for (int i = 0; i < threadCount; i++) {
            handlers[i] = new DefaultPageOperationHandler(i + 1, config);
        }
        if (pohFactory != null) {
            pohFactory.stopHandlers();
            pohFactory.setPageOperationHandlers(handlers);
        } else {
            pohFactory = PageOperationHandlerFactory.create(config, handlers);
        }
        pohFactory.startHandlers();
    }

    @Override
    protected void printRunResult(int loop, long totalTime, long avgTime, String str) {
        if (testConflictOnly)
            printResult(loop,
                    ", row count: " + rowCount + ", thread count: " + threadCount + ", conflict keys: "
                            + conflictKeyCount + ", async write conflict, total time: " + totalTime + " ms, avg time: "
                            + avgTime + " ms, lockCount " + lockCount.get());
        else
            printResult(loop, ", row count: " + rowCount + ", thread count: " + threadCount + ", async" + str
                    + ", total time: " + totalTime + " ms, avg time: " + avgTime + " ms, lockCount " + lockCount.get());
    }

    @Override
    protected BenchTestTask createBenchTestTask(int start, int end) throws Exception {
        lockCount.set(0);
        if (testConflictOnly)
            return new AsyncBTreeConflictBenchTestTask();
        else
            return new AsyncBTreeBenchTestTask(start, end);
    }

    private class AsyncBTreeBenchTestTask extends StorageMapBenchTestTask implements PageOperation {

        PageOperationHandler currentHandler;

        AsyncBTreeBenchTestTask(int start, int end) throws Exception {
            super(start, end);
            currentHandler = pohFactory.getPageOperationHandler();
            currentHandler.handlePageOperation(this);
        }

        @Override
        public PageOperationResult run(PageOperationHandler currentHandler) {
            this.currentHandler = currentHandler;
            super.run();
            return PageOperationResult.SUCCEEDED;
        }

        @Override
        public boolean needCreateThread() {
            return false;
        }

        @Override
        protected void write() throws Exception {
            for (int i = start; i < end; i++) {
                int key;
                if (isRandom())
                    key = randomKeys[i];
                else
                    key = i;
                String value = "value-";// "value-" + key;

                PageOperation po = new Put<>(btreeMap, key, value, ar -> {
                    notifyOperationComplete();
                });

                PageOperationResult result = po.run(currentHandler);
                if (result == PageOperationResult.LOCKED) {
                    lockCount.incrementAndGet();
                    currentHandler.handlePageOperation(po);
                } else if (result == PageOperationResult.RETRY) {
                    currentHandler.handlePageOperation(po);
                }
            }
        }
    }

    private class AsyncBTreeConflictBenchTestTask extends AsyncBTreeBenchTestTask {

        AsyncBTreeConflictBenchTestTask() throws Exception {
            super(0, conflictKeyCount);
        }

        @Override
        protected void write() throws Exception {
            for (int i = 0; i < conflictKeyCount; i++) {
                int key = conflictKeys[i];
                String value = "value-conflict";

                PageOperation po = new Put<>(btreeMap, key, value, ar -> {
                    notifyOperationComplete();
                });
                PageOperationResult result = po.run(currentHandler);
                if (result == PageOperationResult.LOCKED) {
                    lockCount.incrementAndGet();
                    currentHandler.handlePageOperation(po);
                } else if (result == PageOperationResult.RETRY) {
                    currentHandler.handlePageOperation(po);
                }
            }
        }
    }
}
