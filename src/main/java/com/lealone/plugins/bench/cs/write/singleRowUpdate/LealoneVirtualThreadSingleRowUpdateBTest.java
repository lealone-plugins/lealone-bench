/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.bench.cs.write.singleRowUpdate;

public class LealoneVirtualThreadSingleRowUpdateBTest extends SingleRowUpdateBTest {

    public LealoneVirtualThreadSingleRowUpdateBTest() {
        useVirtualThread = true;
    }

    public static void main(String[] args) {
        new LealoneVirtualThreadSingleRowUpdateBTest().start();
    }
}
