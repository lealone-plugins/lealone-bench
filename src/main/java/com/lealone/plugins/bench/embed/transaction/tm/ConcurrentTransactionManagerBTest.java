/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.bench.embed.transaction.tm;

public class ConcurrentTransactionManagerBTest extends TransactionManagerBTest {

    public static void main(String[] args) {
        new ConcurrentTransactionManagerBTest().run(false);
    }
}
