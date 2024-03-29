/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.bench.tpcc.db;

import com.lealone.plugins.bench.tpcc.bench.TpccBench;

public class LealoneBench {
    public static void main(String[] args) {
        System.setProperty("db.config", "lealone/db.properties");
        TpccBench.main(args);
    }
}
