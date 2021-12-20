/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.bench.cs;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Properties;

import org.lealone.bench.BenchTest;

public abstract class ClientServerBTest extends BenchTest {

    protected static void initData(Statement statement) throws Exception {
        statement.executeUpdate("drop table if exists test");
        statement.executeUpdate("create table if not exists test(name varchar, f1 int,f2 int)");

        statement.getConnection().setAutoCommit(false);
        statement.executeUpdate("insert into test values('abc1',1,2)");
        statement.executeUpdate("insert into test values('abc2',2,2)");
        statement.executeUpdate("insert into test values('abc3',3,2)");
        statement.executeUpdate("insert into test values('abc1',1,2)");
        statement.executeUpdate("insert into test values('abc2',2,2)");
        statement.executeUpdate("insert into test values('abc3',3,2)");
        statement.getConnection().commit();
        statement.getConnection().setAutoCommit(true);
    }

    public static Connection getMySQLConnection() throws Exception {
        String db = "test";
        String user = "test";
        String password = "test";
        int port = 3306;
        String url = "jdbc:mysql://localhost:" + port + "/" + db;

        Properties info = new Properties();
        info.put("user", user);
        info.put("password", password);
        // info.put("holdResultsOpenOverStatementClose","true");
        // info.put("allowMultiQueries","true");

        // info.put("useServerPrepStmts", "true");
        // info.put("cachePrepStmts", "true");
        // info.put("rewriteBatchedStatements", "true");
        // info.put("useCompression", "true");
        info.put("serverTimezone", "GMT");

        Connection conn = DriverManager.getConnection(url, info);
        // conn.setAutoCommit(true);
        return conn;
    }
}
