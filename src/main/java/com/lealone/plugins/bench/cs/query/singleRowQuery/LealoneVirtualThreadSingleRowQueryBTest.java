/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.bench.cs.query.singleRowQuery;

public class LealoneVirtualThreadSingleRowQueryBTest extends SingleRowQueryBTest {

    public LealoneVirtualThreadSingleRowQueryBTest() {
        useVirtualThread = true;
    }

    public static void main(String[] args) {
        new LealoneVirtualThreadSingleRowQueryBTest().start();
    }
}
