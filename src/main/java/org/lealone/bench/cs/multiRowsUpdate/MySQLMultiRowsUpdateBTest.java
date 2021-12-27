/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.bench.cs.multiRowsUpdate;

import org.lealone.bench.DbType;

public class MySQLMultiRowsUpdateBTest extends MultiRowsUpdateBTest {

    public static void main(String[] args) {
        new MySQLMultiRowsUpdateBTest().run(DbType.MySQL);
    }
}
