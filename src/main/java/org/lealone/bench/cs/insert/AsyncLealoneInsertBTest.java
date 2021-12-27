/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.bench.cs.insert;

import org.lealone.bench.DbType;

public class AsyncLealoneInsertBTest extends InsertBTest {

    public static void main(String[] args) {
        AsyncLealoneInsertBTest test = new AsyncLealoneInsertBTest();
        test.async = true;
        test.run(DbType.Lealone);
    }
}
