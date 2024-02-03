/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.bench.misc.mongodb;

public class LealoneSyncSingleRowQueryBTest extends MongodbSyncSingleRowQueryBTest {

    public static void main(String[] args) {
        new LealoneSyncSingleRowQueryBTest().run(LEALONE_PORT);
    }

}
