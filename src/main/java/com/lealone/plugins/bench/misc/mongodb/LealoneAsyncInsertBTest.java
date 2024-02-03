/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.bench.misc.mongodb;

public class LealoneAsyncInsertBTest extends MongodbAsyncInsertBTest {

    public static void main(String[] args) {
        new LealoneAsyncInsertBTest().run(LEALONE_PORT);
    }
}
