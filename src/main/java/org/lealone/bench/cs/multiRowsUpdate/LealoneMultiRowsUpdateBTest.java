/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.bench.cs.multiRowsUpdate;

import org.lealone.bench.DbType;

public class LealoneMultiRowsUpdateBTest extends MultiRowsUpdateBTest {

    public static void main(String[] args) {
        AsyncLealoneMultiRowsUpdateBTest test = new AsyncLealoneMultiRowsUpdateBTest();
        test.async = false;
        test.run(DbType.Lealone);
    }
}
