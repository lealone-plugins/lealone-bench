/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.bench.start;

import java.sql.SQLException;
import java.util.ArrayList;

import com.lealone.plugins.bench.BenchTest;

public class H2BenchTestServer {

    public static void main(String[] args) throws SQLException {
        setH2Properties();
        ArrayList<String> list = new ArrayList<String>();

        list.add("-tcp");
        list.add("-tcpPort");
        list.add("9092");
        list.add("-tcpAllowOthers");

        // list.add("-pg");
        // list.add("-pgPort");
        // list.add("9511");
        // list.add("-pgAllowOthers");

        // list.add("-pg");
        // list.add("-tcp");
        // list.add("-web");
        // list.add("-ifExists");
        list.add("-ifNotExists");
        System.out.println("H2 jvm pid: "
                + java.lang.management.ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);
        org.h2.tools.Server.main(list.toArray(new String[list.size()]));
    }

    public static void setH2Properties() {
        String testDir = BenchTest.BENCH_TEST_BASE_DIR;
        // testDir = "F:/target";
        System.setProperty("h2.queryCacheSize", "0");
        // System.setProperty("DATABASE_TO_UPPER", "false");
        System.setProperty("h2.lobInDatabase", "false");
        System.setProperty("h2.lobClientMaxSizeMemory", "1024");
        System.setProperty("java.io.tmpdir", testDir + "/h2/tmp");
        System.setProperty("h2.baseDir", testDir + "/h2");
        // System.setProperty("h2.check2", "true");
        System.setProperty("h2.bindAddress", "localhost");

    }
}
