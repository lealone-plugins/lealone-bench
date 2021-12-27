/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.bench.cs.append;

import org.lealone.bench.DbType;

public class AsyncLealoneAppendBTest extends AppendBTest {

    public static void main(String[] args) {
        AsyncLealoneAppendBTest test = new AsyncLealoneAppendBTest();
        test.async = true;
        test.run(DbType.Lealone);
    }
}
