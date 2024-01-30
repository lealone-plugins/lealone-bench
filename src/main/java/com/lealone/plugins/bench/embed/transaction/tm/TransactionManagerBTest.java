/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.bench.embed.transaction.tm;

import java.util.concurrent.atomic.AtomicLong;

import com.lealone.db.RunMode;
import com.lealone.transaction.Transaction;
import com.lealone.transaction.TransactionEngine;
import com.lealone.transaction.aote.AOTransaction;
import com.lealone.transaction.aote.AOTransactionEngine;
import com.lealone.transaction.aote.tm.ConcurrentTransactionManager;
import com.lealone.transaction.aote.tm.SingleThreadTransactionManager;
import com.lealone.transaction.aote.tm.TransactionManager;

public abstract class TransactionManagerBTest {

    private AOTransactionEngine te = (AOTransactionEngine) TransactionEngine
            .getDefaultTransactionEngine();

    public void run(boolean isSingleThread) {
        for (int n = 0; n < 300; n++) {
            TransactionManager tm = new ConcurrentTransactionManager(te);
            int threadCount = 32;
            Thread[] ts = new Thread[threadCount];
            TransactionManager[] tms = new TransactionManager[threadCount];
            for (int i = 0; i < threadCount; i++) {
                int index = i;
                if (isSingleThread)
                    tms[i] = new SingleThreadTransactionManager(te);
                else
                    tms[i] = tm;
                ts[i] = new Thread(() -> {
                    testTransactionManager(tms[index]);
                });
            }
            long t1 = System.currentTimeMillis();
            for (int i = 0; i < threadCount; i++) {
                ts[i].start();
            }
            for (int i = 0; i < threadCount; i++) {
                try {
                    ts[i].join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            long t2 = System.currentTimeMillis();
            System.out.println(getClass().getSimpleName() + ": threadCount: " + threadCount + ", time: "
                    + (t2 - t1) + " ms");
        }
    }

    private AtomicLong id = new AtomicLong();

    private void testTransactionManager(TransactionManager tm) {
        int size = 8000;
        AOTransaction[] ts = new AOTransaction[size];
        for (int i = 0; i < size; i++) {
            ts[i] = new AOTransaction(te, id.incrementAndGet(), RunMode.CLIENT_SERVER,
                    Transaction.IL_READ_COMMITTED);
            tm.addTransaction(ts[i]);
        }
        for (int i = 0; i < size; i++) {
            tm.removeTransaction(ts[i].getTransactionId(), ts[i].getBitIndex());
        }
    }
}
