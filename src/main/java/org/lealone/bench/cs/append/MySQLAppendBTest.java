/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.bench.cs.append;

import org.lealone.bench.DbType;

public class MySQLAppendBTest extends AppendBTest {

    public static void main(String[] args) {
        new MySQLAppendBTest().run(DbType.MySQL);
    }
}
