/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.bench.tpcc.db;

import com.lealone.plugins.bench.tpcc.bench.TpccBench;

public class H2Bench {
    public static void main(String[] args) {
        System.setProperty("db.config", "h2/db.properties");
        TpccBench.main(args);
    }
}
