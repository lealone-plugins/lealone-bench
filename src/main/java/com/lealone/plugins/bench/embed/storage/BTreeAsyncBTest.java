/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.bench.embed.storage;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import com.lealone.db.scheduler.SchedulerFactory;
import com.lealone.storage.aose.btree.BTreeMap;

// -Xms512M -Xmx512M -XX:+PrintGCDetails -XX:+PrintGCTimeStamps
public class BTreeAsyncBTest extends StorageMapBTest {

    public static void main(String[] args) throws Exception {
        new BTreeAsyncBTest().run();
    }

    private final AtomicInteger lockCount = new AtomicInteger(0);
    private BTreeMap<Integer, String> btreeMap;

    @Override
    protected void init() {
        if (!inited.compareAndSet(false, true))
            return;
        initConfig();
        openStorage();
        map = btreeMap = storage.openBTreeMap(BTreeAsyncBTest.class.getSimpleName(), getIntType(),
                getStringType(), null);
    }

    @Override
    void singleThreadWrite(boolean random) {
        long t1 = System.currentTimeMillis();
        write(random, 0, rowCount);
        long t2 = System.currentTimeMillis();
        printResult("single-thread " + (random ? "random" : "serial") + " write time: " + (t2 - t1)
                + " ms, count: " + rowCount);
    }

    @Override
    void write(boolean random, int start, int end) {
        int rowCount = end - start;
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger counter = new AtomicInteger(rowCount);
        for (int i = start; i < end; i++) {
            int key = random ? randomKeys[i] : i;
            btreeMap.put(key, "valueaaa", ar -> {
                notifyOperationComplete();
                if (counter.decrementAndGet() == 0) {
                    latch.countDown();
                }
            });
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void testConflict(int loop) {
        testConflict(loop, true);
    }

    @Override
    protected void printRunResult(int loop, long totalTime, long avgTime, String str) {
        if (testConflictOnly)
            printResult(loop,
                    ", row count: " + rowCount + ", thread count: " + threadCount + ", conflict keys: "
                            + conflictKeyCount + ", async write conflict, total time: " + totalTime
                            + " ms, avg time: " + avgTime + " ms, lockCount " + lockCount.get());
        else
            printResult(loop,
                    ", row count: " + rowCount + ", thread count: " + threadCount + ", async" + str
                            + ", total time: " + totalTime + " ms, avg time: " + avgTime
                            + " ms, lockCount " + lockCount.get());
    }

    @Override
    protected BenchTestTask createBenchTestTask(int start, int end) throws Exception {
        lockCount.set(0);
        BenchTestTask task;
        if (testConflictOnly)
            task = new AsyncBTreeConflictBenchTestTask();
        else
            task = new AsyncBTreeBenchTestTask(start, end);
        if (runTaskInScheduler) {
            SchedulerFactory sf = SchedulerFactory.getDefaultSchedulerFactory();
            sf.getScheduler().handle(task);
        }
        return task;
    }

    private class AsyncBTreeBenchTestTask extends StorageMapBenchTestTask {

        AsyncBTreeBenchTestTask(int start, int end) throws Exception {
            super(start, end);
        }

        @Override
        protected void write() throws Exception {
            BTreeAsyncBTest.this.write(isRandom(), start, end);
        }

        @Override
        public boolean needCreateThread() {
            return !runTaskInScheduler;
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
                btreeMap.put(key, value, ar -> {
                    notifyOperationComplete();
                });
            }
        }
    }
}
