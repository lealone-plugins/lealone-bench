/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.bench.cs.columnlock;

import java.sql.Connection;

public class H2ColumnLockBTest extends ColumnLockBTest {

    public static void main(String[] args) throws Throwable {
        new H2ColumnLockBTest().run();
    }

    @Override
    public Connection getConnection() throws Exception {
        return getConnection(9511);
    }
}
