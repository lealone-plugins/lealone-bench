/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.bench.cs.append;

import java.sql.Connection;

public class MySQLAppendBTest extends AppendBTest {

    public static void main(String[] args) throws Throwable {
        new MySQLAppendBTest().run();
    }

    @Override
    public Connection getConnection() throws Exception {
        return getMySQLConnection();
    }
}
