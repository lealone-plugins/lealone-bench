/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.bench.embed.storage;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.lealone.db.value.ValueInt;
import org.lealone.db.value.ValueString;
import org.lealone.storage.aose.btree.BTreeMap;
import org.lealone.storage.aose.btree.page.Page;
import org.lealone.storage.aose.btree.page.PageOperations.Put;
import org.lealone.storage.aose.btree.page.PageReference;
import org.lealone.storage.page.DefaultPageOperationHandler;
import org.lealone.storage.page.PageOperation;
import org.lealone.storage.page.PageOperationHandler;
import org.lealone.storage.page.PageOperationHandlerFactory;

// -Xms512M -Xmx512M -XX:+PrintGCDetails -XX:+PrintGCTimeStamps
public class AsyncBTreeBTest extends StorageMapBTest {

    public static void main(String[] args) throws Exception {
        new AsyncBTreeBTest().run();
    }

    private final AtomicInteger index = new AtomicInteger(0);
    private BTreeMap<Integer, String> btreeMap;
    private DefaultPageOperationHandler[] handlers;

    @Override
    protected void resetFields() {
        super.resetFields();
        index.set(0);
    }

    @Override
    protected void testWrite(int loop) {
        multiThreadsRandomWriteAsync(loop);
        multiThreadsSerialWriteAsync(loop);
    }

    @Override
    protected void testRead(int loop) {
        multiThreadsRandomRead(loop);
        multiThreadsSerialRead(loop);

        // multiThreadsRandomReadAsync(loop);
        // multiThreadsSerialReadAsync(loop);
    }

    @Override
    protected void testConflict(int loop) {
        testConflict(loop, true);
    }

    @Override
    protected void beforeRun() {
        createPageOperationHandlers();
        super.beforeRun();
        // printLeafPageOperationHandlerPercent();
        // printShiftCount(conflictKeys);
    }

    void printShiftCount(int[] keys) {
        HashMap<PageOperationHandler, Integer> map = new HashMap<>();
        for (int key : keys) {
            Page p = btreeMap.gotoLeafPage(key);
            PageOperationHandler handler = p.getHandler();
            Integer count = map.get(handler);
            if (count == null)
                count = 1;
            else
                count++;
            map.put(handler, count);
        }

        System.out.println("key count: " + keys.length);
        for (HashMap.Entry<PageOperationHandler, Integer> e : map.entrySet()) {
            String percent = String.format("%#.2f", (e.getValue() * 1.0 / keys.length * 100));
            System.out.println(e.getKey() + " percent: " + percent + "%");
        }
        System.out.println();
    }

    void printLeafPageOperationHandlerPercent() {
        Page root = btreeMap.getRootPage();
        HashMap<PageOperationHandler, Integer> map = new HashMap<>();
        AtomicLong leafPageCount = new AtomicLong(0);
        if (root.isLeaf()) {
            map.put(root.getHandler(), 1);
            leafPageCount.incrementAndGet();
        } else {
            findLeafPage(root, map, leafPageCount);
        }
        System.out.println("leaf page count: " + leafPageCount.get());
        System.out.println("handler factory: " + storage.getPageOperationHandlerFactory().getClass().getSimpleName());
        for (HashMap.Entry<PageOperationHandler, Integer> e : map.entrySet()) {
            String percent = String.format("%#.2f", (e.getValue() * 1.0 / leafPageCount.get() * 100));
            System.out.println(e.getKey() + " percent: " + percent + "%");
        }
        System.out.println();
    }

    private void findLeafPage(Page p, HashMap<PageOperationHandler, Integer> map, AtomicLong leafPageCount) {
        if (p.isNode()) {
            for (PageReference ref : p.getChildren()) {
                Page child = ref.getPage();
                if (child.isLeaf()) {
                    PageOperationHandler handler = child.getHandler();
                    // System.out.println("handler: " + handler);
                    Integer count = map.get(handler);
                    if (count == null)
                        count = 1;
                    else
                        count++;
                    map.put(handler, count);
                    leafPageCount.incrementAndGet();
                } else {
                    findLeafPage(child, map, leafPageCount);
                }
            }
        }
    }

    @Override
    protected void init() {
        if (!inited.compareAndSet(false, true))
            return;
        initConfig();
        createPageOperationHandlers();
        openStorage(false);
        openMap();
    }

    private void createPageOperationHandlers() {
        handlers = new DefaultPageOperationHandler[threadCount];
        for (int i = 0; i < threadCount; i++) {
            handlers[i] = new DefaultPageOperationHandler(i, config);
        }
        PageOperationHandlerFactory f = PageOperationHandlerFactory.create(config, handlers);
        f.stopHandlers();
        f.setPageOperationHandlers(handlers);
        f.startHandlers();
    }

    @Override
    protected void openMap() {
        if (map == null || map.isClosed()) {
            map = btreeMap = storage.openBTreeMap(AsyncBTreeBTest.class.getSimpleName(), ValueInt.type,
                    ValueString.type, null);
        }
    }

    @Override
    protected void printRunResult(int loop, long totalTime, long avgTime, String str) {
        String shiftStr = getShiftStr();

        if (testConflictOnly)
            printResult(loop,
                    ", row count: " + rowCount + ", thread count: " + threadCount + ", conflict keys: "
                            + conflictKeyCount + shiftStr + ", async write conflict, total time: " + totalTime
                            + " ms, avg time: " + avgTime + " ms");
        else
            printResult(loop, ", row count: " + rowCount + ", thread count: " + threadCount + shiftStr + ", async" + str
                    + ", total time: " + totalTime + " ms, avg time: " + avgTime + " ms");
    }

    // 异步场景下线程移交PageOperation的次数
    private String getShiftStr() {
        String shiftStr = "";
        long shiftSum = 0;
        for (int i = 0; i < threadCount; i++) {
            DefaultPageOperationHandler h = handlers[i];
            shiftSum += h.getShiftCount();
        }
        shiftStr = ", shift: " + shiftSum;
        return shiftStr;
    }

    @Override
    protected BenchTestTask createBenchTestTask(int start, int end) throws Exception {
        if (testConflictOnly)
            return new AsyncBTreeConflictBenchTestTask();
        else
            return new AsyncBTreeBenchTestTask(start, end);
    }

    class AsyncBTreeBenchTestTask extends StorageMapBenchTestTask implements PageOperation {

        PageOperationHandler currentHandler;

        AsyncBTreeBenchTestTask(int start, int end) throws Exception {
            super(start, end);
            DefaultPageOperationHandler h = handlers[index.getAndIncrement()];
            h.reset(false);
            h.handlePageOperation(this);
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
        protected void read() throws Exception {
            for (int i = start; i < end; i++) {
                int key;
                if (isRandom())
                    key = randomKeys[i];
                else
                    key = i;
                map.get(key, ar -> {
                    notifyOperationComplete();
                });
            }
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
                po.run(currentHandler);
                // PageOperationResult result = po.run(currentHandler);
                // if (result == PageOperationResult.SHIFTED) {
                // shiftCount++;
                // }
                // currentHandler.handlePageOperation(po);
            }
        }
    }

    class AsyncBTreeConflictBenchTestTask extends AsyncBTreeBenchTestTask {

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
                po.run(currentHandler);
            }
        }
    }
}
