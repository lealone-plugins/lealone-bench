/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.bench.columnlock;

import java.sql.Connection;
import java.sql.Statement;

import org.lealone.db.ConnectionSetting;
import org.lealone.db.Constants;

public class LealoneColumnLockPerfTest extends ColumnLockPerfTest {

    public static void main(String[] args) throws Exception {
        Connection conn = getSyncConnection();
        Statement statement = conn.createStatement();
        statement.executeUpdate("set QUERY_CACHE_SIZE 0");
        statement.close();
        conn.close();

        new LealoneColumnLockPerfTest().run();
    }

    @Override
    public Connection getConnection() throws Exception {
        return getSyncConnection();
    }

    public static Connection getSyncConnection() throws Exception {
        String url = "jdbc:lealone:tcp://localhost:" + Constants.DEFAULT_TCP_PORT + "/lealone";
        url += "?" + ConnectionSetting.NETWORK_TIMEOUT + "=" + Integer.MAX_VALUE;
        Connection conn = getConnection(url, "root", "");
        // conn= getConnection(PgServer.DEFAULT_PORT);
        return conn;
    }
}
