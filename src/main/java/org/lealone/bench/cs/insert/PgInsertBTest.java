/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.bench.cs.insert;

import java.sql.Connection;

public class PgInsertBTest extends InsertBTest {

    public static void main(String[] args) throws Throwable {
        new PgInsertBTest().run();
    }

    @Override
    public Connection getConnection() throws Exception {
        return getConnection(5432);
    }
}
