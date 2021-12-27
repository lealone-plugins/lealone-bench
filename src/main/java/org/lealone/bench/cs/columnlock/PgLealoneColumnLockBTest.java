/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.bench.cs.columnlock;

import java.sql.Connection;

import org.lealone.bench.DbType;
import org.lealone.xsql.postgresql.server.PgServer;

public class PgLealoneColumnLockBTest extends ColumnLockBTest {

    public static void main(String[] args) {
        new PgLealoneColumnLockBTest().run(DbType.Lealone);
    }

    @Override
    public Connection getConnection() throws Exception {
        return getPgConnection(PgServer.DEFAULT_PORT);
    }
}
