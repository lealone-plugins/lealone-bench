/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.bench.cs.insert;

import java.sql.Connection;

public class MySQLInsertBTest extends InsertBTest {

    public static void main(String[] args) throws Throwable {
        new MySQLInsertBTest().run();
    }

    @Override
    public Connection getConnection() throws Exception {
        return getMySQLConnection();
    }
}
