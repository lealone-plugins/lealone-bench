/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.bench.cs.columnlock;

import org.lealone.bench.DbType;

public class AsyncLealoneColumnLockBTest extends ColumnLockBTest {

    public static void main(String[] args) {
        AsyncLealoneColumnLockBTest test = new AsyncLealoneColumnLockBTest();
        test.async = true;
        test.run(DbType.Lealone);
    }
}
