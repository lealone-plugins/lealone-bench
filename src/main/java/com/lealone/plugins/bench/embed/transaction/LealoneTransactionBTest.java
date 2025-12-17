/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.bench.embed.transaction;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.lealone.storage.aose.AOStorage;
import com.lealone.storage.aose.AOStorageBuilder;
import com.lealone.test.aote.TransactionEngineTest;
import com.lealone.transaction.Transaction;
import com.lealone.transaction.TransactionEngine;
import com.lealone.transaction.TransactionMap;

public class LealoneTransactionBTest extends TransactionBTest {

    public static void main(String[] args) throws Exception {
        // printMemoryUsage();
        LealoneTransactionBTest test = new LealoneTransactionBTest();
        run(test);
    }

    protected AOStorage storage;
    protected String storagePath;

    private final HashMap<String, String> config = new HashMap<>();
    private final AtomicInteger index = new AtomicInteger(0);
    private TransactionEngine te;

    @Override
    protected void resetFields() {
        super.resetFields();
        index.set(0);
    }

    @Override
    protected void init() throws Exception {
        AOStorageBuilder builder = new AOStorageBuilder(config);
        storagePath = joinDirs("lealone", "aose");
        int pageSize = 16 * 1024;
        builder.storagePath(storagePath).compress().pageSize(pageSize).minFillRate(30);
        storage = builder.openStorage();

        initTransactionEngineConfig(config);
        te = TransactionEngineTest.getTransactionEngine(config);

        singleThreadSerialWrite();
    }

    @Override
    protected void destroy() throws Exception {
        te.close();
        storage.close();
    }

    private void singleThreadSerialWrite() {
        Transaction t = te.beginTransaction();
        TransactionMap<Integer, String> map = t.openMap(mapName, storage);
        map.clear();
        long t1 = System.currentTimeMillis();
        for (int i = 0; i < rowCount; i++) {
            map.put(i, "valueaaa");
        }
        t.commit();
        long t2 = System.currentTimeMillis();

        printResult("single-thread serial write time: " + (t2 - t1) + " ms, row count: " + map.size());
    }

    @Override
    protected void write(int start, int end) throws Exception {
        Transaction t = te.beginTransaction();
        TransactionMap<Integer, String> map = t.openMap(mapName, storage);
        for (int i = start; i < end; i++) {
            Integer key;
            if (isRandom())
                key = randomKeys[i];
            else
                key = i;
            String value = "value-";// "value-" + key;
            // map.put(key, value);

            Transaction t2 = te.beginTransaction();
            TransactionMap<Integer, String> m = map.getInstance(t2);
            m.tryUpdate(key, value);
            // m.put(key, value);
            t2.commit();
            // System.out.println(getName() + " key:" + key);
            notifyOperationComplete();
        }
    }
}
