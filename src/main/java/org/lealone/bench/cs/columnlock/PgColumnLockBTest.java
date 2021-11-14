/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.bench.cs.columnlock;

import java.sql.Connection;

public class PgColumnLockBTest extends ColumnLockBTest {

    public static void main(String[] args) throws Throwable {
        new PgColumnLockBTest().run();
    }

    @Override
    public Connection getConnection() throws Exception {
        return getConnection(5432);
    }
}
