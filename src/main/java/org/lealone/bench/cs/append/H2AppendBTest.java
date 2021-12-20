/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.bench.cs.append;

import java.sql.Connection;

public class H2AppendBTest extends AppendBTest {

    public static void main(String[] args) throws Throwable {
        new H2AppendBTest().run();
    }

    @Override
    public Connection getConnection() throws Exception {
        return getH2Connection();
        // return getConnection(9511);
    }

    public static Connection getH2Connection() throws Exception {
        String url = "jdbc:h2:tcp://localhost:9092/mydb";
        return getConnection(url, "sa", "");
    }
}
