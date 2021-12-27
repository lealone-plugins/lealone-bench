/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.bench.cs.columnlock;

import org.lealone.bench.DbType;

public class PgColumnLockBTest extends ColumnLockBTest {

    public static void main(String[] args) {
        new PgColumnLockBTest().run(DbType.PostgreSQL);
    }
}
