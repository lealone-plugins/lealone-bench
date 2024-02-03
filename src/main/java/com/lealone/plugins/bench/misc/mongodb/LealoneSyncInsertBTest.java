/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.bench.misc.mongodb;

public class LealoneSyncInsertBTest extends MongodbSyncInsertBTest {

    public static void main(String[] args) {
        new LealoneSyncInsertBTest().run(LEALONE_PORT);
    }
}
