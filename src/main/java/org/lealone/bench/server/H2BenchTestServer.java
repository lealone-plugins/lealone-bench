/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.bench.server;

import java.sql.SQLException;
import java.util.ArrayList;

import org.lealone.bench.BenchTest;

public class H2BenchTestServer {

    public static void main(String[] args) throws SQLException {
        setH2Properties();

        ArrayList<String> list = new ArrayList<String>();
        // list.add("-tcp");
        // //list.add("-tool");
        // org.h2.tools.Server.main(list.toArray(new String[list.size()]));
        //
        // list.add("-tcp");
        // list.add("-tcpPort");
        // list.add("9092");

        // 测试org.h2.server.TcpServer.checkKeyAndGetDatabaseName(String)
        // list.add("-key");
        // list.add("mydb");
        // list.add("mydatabase");

        // list.add("-pg");
        list.add("-tcp");
        // list.add("-web");
        // list.add("-ifExists");
        list.add("-ifNotExists");
        list.add("-tcpAllowOthers");
        org.h2.tools.Server.main(list.toArray(new String[list.size()]));
    }

    public static void setH2Properties() {
        // System.setProperty("DATABASE_TO_UPPER", "false");
        System.setProperty("h2.lobInDatabase", "false");
        System.setProperty("h2.lobClientMaxSizeMemory", "1024");
        System.setProperty("java.io.tmpdir", BenchTest.BENCH_TEST_BASE_DIR + "/h2/tmp");
        System.setProperty("h2.baseDir", BenchTest.BENCH_TEST_BASE_DIR + "/h2");
        // System.setProperty("h2.check2", "true");
    }
}
