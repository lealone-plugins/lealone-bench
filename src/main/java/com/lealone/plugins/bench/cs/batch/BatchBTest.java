/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.bench.cs.batch;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

import com.lealone.plugins.bench.DbType;
import com.lealone.plugins.bench.cs.write.ClientServerWriteBTest;

public class BatchBTest extends ClientServerWriteBTest {

    protected boolean init;
    protected boolean useRandom;
    protected int batchCount = 1000;
    protected String tableName;

    protected BatchBTest() {
        benchTestLoop = 10;
        rowCount = 10 * 10000;
        init = true;
        // useRandom = true;
        tableName = "BatchBTest";
    }

    @Override
    public void run() throws Exception {
        if (init) {
            init0();
        }
        run0();
    }

    private void init0() throws Exception {
        Connection conn = getConnection();
        Statement statement = conn.createStatement();
        createTable(statement, false);
        // createIndex(statement, "f1", false);
        // createIndex(statement, "f2", false);
        batchPreparedInsert0(conn, "batchPreparedInsert");
        close(statement, conn);
    }

    private void run0() throws Exception {
        for (int i = 0; i < benchTestLoop; i++) {
            Connection conn = getConnection();
            Statement statement = conn.createStatement();
            run0(conn, statement);
            close(statement, conn);
        }
    }

    private void run0(Connection conn, Statement statement) throws Exception {
        // batchInsert(statement);
        // batchPreparedInsert(conn, statement);
        // batchAppend(statement);
        // batchPreparedAppend(conn, statement);
        // batchUpdate(statement);
        batchPreparedUpdate(conn);
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

    private void batchInsert0(Statement statement, String method) throws Exception {
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

    private void batchPreparedInsert0(Connection conn, String method) throws Exception {
        long t1 = System.nanoTime();
        String sql = "INSERT INTO " + tableName + "(pk, f1, f2) VALUES(?,?,?)";
        PreparedStatement ps = conn.prepareStatement(sql);
        for (int i = 1; i <= rowCount; i++) {
            ps.setInt(1, i);
            int f = useRandom ? random.nextInt(rowCount) : i * 10;
            ps.setInt(2, f);
            ps.setString(3, "v-" + f);
            ps.addBatch();
            if (i % batchCount == 0)
                ps.executeBatch();
        }
        ps.close();
        println(method, t1);
    }

    public void batchInsert(Statement statement) throws Exception {
        createTable(statement, false);
        batchInsert0(statement, "batchInsert");
    }

    public void batchPreparedInsert(Connection conn, Statement statement) throws Exception {
        createTable(statement, false);
        batchPreparedInsert0(conn, "batchPreparedInsert");
    }

    public void batchAppend(Statement statement) throws Exception {
        createTable(statement, true);
        batchInsert0(statement, "batchAppend");
    }

    public void batchPreparedAppend(Connection conn, Statement statement) throws Exception {
        createTable(statement, true);
        batchPreparedInsert0(conn, "batchPreparedAppend");
    }

    public void batchUpdate(Statement statement) throws Exception {
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
        println("batchUpdate", t1);
    }

    public void batchPreparedUpdate(Connection conn) throws Exception {
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
        println("batchPreparedUpdate", t1);
    }

    private void println(String method, long t1) {
        long t2 = System.nanoTime();
        System.out.println(getBTestName() + " " + method + " row count: " + rowCount + " total time: "
                + TimeUnit.NANOSECONDS.toMillis(t2 - t1) + " ms");
    }
}
