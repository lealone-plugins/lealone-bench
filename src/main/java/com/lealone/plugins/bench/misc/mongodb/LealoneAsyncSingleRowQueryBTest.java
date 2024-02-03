/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.bench.misc.mongodb;

public class LealoneAsyncSingleRowQueryBTest extends MongodbAsyncSingleRowQueryBTest {

    public static void main(String[] args) {
        new LealoneAsyncSingleRowQueryBTest().run(LEALONE_PORT);
    }

}
