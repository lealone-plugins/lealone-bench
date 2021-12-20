/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.bench.cs.columnlock;

import java.sql.Connection;

public class MySQLColumnLockBTest extends ColumnLockBTest {

    public static void main(String[] args) throws Throwable {
        new MySQLColumnLockBTest().run();
    }

    @Override
    public Connection getConnection() throws Exception {
        return getMySQLConnection();
    }
}
