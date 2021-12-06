/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.bench.cs.multiRowsUpdate;

import java.sql.Connection;
import java.sql.Statement;

import org.lealone.bench.cs.async.lealone.AsyncLealoneBTest;
import org.lealone.xsql.postgresql.server.PgServer;

public class LealoneMultiRowsUpdateBTest extends MultiRowsUpdateBTest {

    public static void main(String[] args) throws Exception {
        Connection conn = getSyncConnection();
        Statement statement = conn.createStatement();
        statement.executeUpdate("set QUERY_CACHE_SIZE 0");
        statement.close();
        conn.close();

        new LealoneMultiRowsUpdateBTest().run();
    }

    @Override
    public Connection getConnection() throws Exception {
        // return getPgConnection();
        return getSyncConnection();
    }

    public static Connection getSyncConnection() throws Exception {
        String url = AsyncLealoneBTest.getUrl();
        Connection conn = getConnection(url, "root", "");
        // conn= getConnection(PgServer.DEFAULT_PORT);
        return conn;
    }

    public static Connection getPgConnection() throws Exception {
        return getConnection(PgServer.DEFAULT_PORT);
    }
}
