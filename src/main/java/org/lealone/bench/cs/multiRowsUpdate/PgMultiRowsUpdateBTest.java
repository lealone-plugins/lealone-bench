/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.bench.cs.multiRowsUpdate;

import org.lealone.bench.DbType;

public class PgMultiRowsUpdateBTest extends MultiRowsUpdateBTest {

    public static void main(String[] args) {
        new PgMultiRowsUpdateBTest().run(DbType.PostgreSQL);
    }
}
