/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.bench.cs.append;

import java.sql.Connection;

public class PgAppendBTest extends AppendBTest {

    public static void main(String[] args) throws Throwable {
        new PgAppendBTest().run();
    }

    @Override
    public Connection getConnection() throws Exception {
        return getConnection(5432);
    }
}
