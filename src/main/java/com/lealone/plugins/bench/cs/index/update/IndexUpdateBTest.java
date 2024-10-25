/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.bench.cs.index.update;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

import com.lealone.plugins.bench.DbType;
import com.lealone.plugins.bench.cs.write.ClientServerWriteBTest;

public class IndexUpdateBTest extends ClientServerWriteBTest {

    protected boolean init;
    protected boolean useRandom;
    protected int batchCount = 200;
    protected String tableName;

    protected IndexUpdateBTest() {
        benchTestLoop = 20;
        rowCount = 10 * 10000;
        init = true;
        // useRandom = true;
        tableName = "IndexUpdateBTest";
    }

    @Override
    public void run() throws Exception {
        for (int i = 0; i < benchTestLoop; i++) {
            init0();
            run0();
        }
    }

    private void init0() throws Exception {
        Connection conn = getConnection();
        Statement statement = conn.createStatement();
        createTable(statement, false);
        createIndex(statement, "f1", false);
        createIndex(statement, "f2", false);
        createIndex(statement, "f2,f1", false);
        close(statement, conn);
    }

    private void run0() throws Exception {
        Connection conn = getConnection();
        Statement statement = conn.createStatement();
        run0(conn, statement);
        close(statement, conn);
    }

    private void run0(Connection conn, Statement statement) throws Exception {
        // insert(statement);
        preparedInsert(conn, statement);
        // append(statement);
        // preparedAppend(conn, statement);
        // update(statement);
        // preparedUpdate(conn);
    }

    public void createTable(Statement statement) throws Exception {
        createTable(statement, false);
    }

    public void createTable(Statement statement, boolean append) throws Exception {
        statement.executeUpdate("drop table if exists " + tableName);
        String sql = "create table if not exists " + tableName + "(pk int"
                + (!append ? " primary key" : "") + ", f1 int, f2 varchar(30))";
        statement.executeUpdate(sql);
    }

    public void createIndex(Statement statement, String f) throws Exception {
        createIndex(statement, f, true);
    }

    public void createIndex(Statement statement, String f, boolean print) throws Exception {
        long t1 = System.nanoTime();
        String indexName = tableName + "_idx_" + f.replace(',', '_');
        String dsql = "DROP INDEX IF EXISTS " + indexName;
        String csql = "CREATE INDEX IF NOT EXISTS " + indexName + " ON " + tableName + "(" + f + ")";
        if (dbType == DbType.MYSQL) {
            dsql = "DROP INDEX " + indexName + " on " + tableName;
            csql = "CREATE INDEX " + indexName + " ON " + tableName + "(" + f + ")";
        }
        try {
            statement.executeUpdate(dsql);
        } catch (Exception e) {
            // e.printStackTrace();
        }
        try {
            statement.executeUpdate(csql);
        } catch (Exception e) {
            // e.printStackTrace();
        }
        if (print) {
            println("createIndex", t1);
        }
    }

    private void insert0(Statement statement, String method) throws Exception {
        long t1 = System.nanoTime();
        for (int i = 1; i <= rowCount; i++) {
            StringBuilder sql = new StringBuilder();
            sql.append("INSERT INTO ").append(tableName).append("(pk, f1, f2) VALUES(");
            sql.append(i).append(',');
            int f = useRandom ? random.nextInt(rowCount) : i * 10;
            sql.append(f).append(',');
            sql.append("'v-").append(f).append("')");
            statement.addBatch(sql.toString());
            if (i % batchCount == 0)
                statement.executeBatch();
        }
        println(method, t1);
    }

    private void preparedInsert0(Connection conn, String method) throws Exception {
        long t1 = System.nanoTime();
        String sql = "INSERT INTO " + tableName + "(pk, f1, f2) VALUES(?,?,?)";
        PreparedStatement ps = conn.prepareStatement(sql);
        for (int i = 1; i <= rowCount; i++) {
            ps.setInt(1, i);
            int f = useRandom ? random.nextInt(rowCount) : i * 10;
            ps.setInt(2, f);
            ps.setString(3, "v-" + f);
            // ps.executeUpdate();
            ps.addBatch();
            if (i % batchCount == 0)
                ps.executeBatch();
        }
        ps.close();
        println(method, t1);
    }

    public void insert(Statement statement) throws Exception {
        // createTable(statement, false);
        insert0(statement, "insert");
    }

    public void preparedInsert(Connection conn, Statement statement) throws Exception {
        // createTable(statement, false);
        preparedInsert0(conn, "preparedInsert");
    }

    public void append(Statement statement) throws Exception {
        // createTable(statement, true);
        insert0(statement, "append");
    }

    public void preparedAppend(Connection conn, Statement statement) throws Exception {
        // createTable(statement, true);
        preparedInsert0(conn, "preparedAppend");
    }

    public void update(Statement statement) throws Exception {
        long t1 = System.nanoTime();
        for (int i = 1; i <= rowCount; i++) {
            StringBuilder sql = new StringBuilder();
            sql.append("UPDATE ").append(tableName).append(" set f1=");
            sql.append(i * 100).append(", f2=");
            sql.append("'v-").append(i * 100).append("' where pk=");
            int f = useRandom ? random.nextInt(rowCount) : i;
            sql.append(f);
            statement.addBatch(sql.toString());
            if (i % batchCount == 0)
                statement.executeBatch();
        }
        println("update", t1);
    }

    public void preparedUpdate(Connection conn) throws Exception {
        long t1 = System.nanoTime();
        String sql = "UPDATE " + tableName + " set f1=?, f2=? where pk=?";
        PreparedStatement ps = conn.prepareStatement(sql);
        for (int i = 1; i <= rowCount; i++) {
            ps.setInt(1, i * 100);
            ps.setString(2, "v-" + (i * 100));
            int f = useRandom ? random.nextInt(rowCount) : i;
            ps.setInt(3, f);
            ps.addBatch();
            if (i % batchCount == 0)
                ps.executeBatch();
        }
        ps.close();
        println("preparedUpdate", t1);
    }

    private void println(String method, long t1) {
        long t2 = System.nanoTime();
        System.out.println(getBTestName() + " " + method + " row count: " + rowCount + " total time: "
                + TimeUnit.NANOSECONDS.toMillis(t2 - t1) + " ms");
    }
}
