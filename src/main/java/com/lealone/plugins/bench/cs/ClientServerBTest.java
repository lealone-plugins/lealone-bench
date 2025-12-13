/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.bench.cs;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.sqlite.SQLiteConfig;

import com.lealone.db.ConnectionSetting;
import com.lealone.db.Constants;
import com.lealone.db.DbSetting;
import com.lealone.db.SysProperties;
import com.lealone.plugins.bench.BenchTest;
import com.lealone.plugins.bench.DbType;

public abstract class ClientServerBTest extends BenchTest {

    static {
        System.setProperty("lealone.server.cached.objects", "10000000");
        System.setProperty("h2.serverCachedObjects", "10000000");
    }

    protected DbType dbType;
    protected boolean disableLealoneQueryCache = true;

    protected int benchTestLoop = 10;
    protected int outerLoop = 15;
    protected int innerLoop = 100;
    protected int sqlCountPerInnerLoop = 500;
    protected boolean printInnerLoopResult;
    protected boolean async;
    protected boolean autoCommit = true;
    protected boolean batch;
    protected boolean prepare;
    protected boolean reinit = true;
    protected String[] sqls;

    protected AtomicInteger id = new AtomicInteger();
    protected Random random = new Random();

    protected AtomicInteger counterTop = new AtomicInteger(sqlCountPerInnerLoop * innerLoop);
    protected CountDownLatch latchTop = new CountDownLatch(1);

    protected ExecutorService executorService;

    protected boolean embedded;

    public void start() {
        String name = getBTestName();
        DbType dbType;
        if (name.startsWith("AsyncLealone")) {
            dbType = DbType.LEALONE;
            async = true;
        } else if (name.startsWith("Lealone")) {
            dbType = DbType.LEALONE;
            async = false;
        } else if (name.startsWith("H2")) {
            dbType = DbType.H2;
        } else if (name.startsWith("MySQL")) {
            dbType = DbType.MYSQL;
        } else if (name.startsWith("Pg")) {
            dbType = DbType.POSTGRESQL;
        } else if (name.startsWith("SQLite")) {
            dbType = DbType.SQLITE;
        } else if (name.startsWith("LM")) {
            dbType = DbType.LM;
        } else if (name.startsWith("LP")) {
            dbType = DbType.LP;
        } else {
            throw new RuntimeException("Unsupported BTestName: " + name);
        }
        this.dbType = dbType;
        try {
            run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void run() throws Exception {
        // 把一个Runnable任务扔给Executors.newFixedThreadPool()跑要比直接用Thread慢很多
        // executorService = Executors.newFixedThreadPool(threadCount);
        Connection[] conns = new Connection[threadCount];
        for (int i = 0; i < threadCount; i++) {
            Connection conn = getConnection();
            conns[i] = conn;
        }
        for (int i = 0; i < benchTestLoop; i++) {
            if (reinit || i == 0) {
                id.set(0);
                init();
            }
            run(threadCount, conns);
        }
        for (int i = 0; i < threadCount; i++) {
            close(conns[i]);
        }
        // executorService.shutdown();
    }

    protected void run(int threadCount, Connection[] conns) throws Exception {
        // Connection[] conns = new Connection[threadCount];
        // for (int i = 0; i < threadCount; i++) {
        // Connection conn = getConnection();
        // conns[i] = conn;
        // }

        // if (warmUpEnabled()) {
        // for (int i = 0; i < 2; i++) {
        // run(threadCount, conns, true);
        // }
        // }
        long t1 = System.currentTimeMillis();
        for (int i = 0; i < outerLoop; i++) {
            run(threadCount, conns, false);
        }
        long t2 = System.currentTimeMillis();
        System.out.println(getBTestName() + " sql count: "
                + (outerLoop * threadCount * innerLoop * sqlCountPerInnerLoop) + ", total time: "
                + (t2 - t1) + " ms");
        // for (int i = 0; i < threadCount; i++) {
        // close(conns[i]);
        // }
    }

    protected boolean warmUpEnabled() {
        return true;
    }

    protected void run(int threadCount, Connection[] conns, boolean warmUp) throws Exception {
        ClientServerBTestThread[] threads = new ClientServerBTestThread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            threads[i] = createBTestThread(i, conns[i]);
        }
        counterTop.set(threadCount * sqlCountPerInnerLoop * innerLoop);
        latchTop = new CountDownLatch(1);
        long t1 = System.currentTimeMillis();
        long totalTime = 0;
        @SuppressWarnings("unused")
        ArrayList<Future<?>> futures = new ArrayList<>(threadCount);
        for (int i = 0; i < threadCount; i++) {
            threads[i].setCloseConn(false);
            threads[i].start();
            // futures.add(executorService.submit(threads[i]));
        }

        // latchTop.await();
        for (int i = 0; i < threadCount; i++) {
            // futures.get(i).get();
            threads[i].join();
            totalTime += threads[i].getTotalTime();

            // System.out.println(threads[i].getName() + " start time: " //
            // + toMillis(threads[i].getStartTime()) + ", end time: " //
            // + toMillis(threads[i].getEndTime()) + ", total time: " //
            // + toMillis(threads[i].getTotalTime()));

            // System.out.println(getBTestName() + " sql count: " + (innerLoop * sqlCountPerInnerLoop)
            // + " start: " + threads[i].t1 + " end: " + threads[i].t2 + ", thread livecycle: "
            // + (threads[i].t2 - threads[i].t1) + " ms, sql execute time: "
            // + toMillis(threads[i].getTotalTime()) + " ms");
        }
        long t2 = System.currentTimeMillis();
        long avgTime = toMillis(totalTime / threadCount);
        totalTime = (t2 - t1);
        System.out.println(
                getBTestName() + " sql count: " + (threadCount * innerLoop * sqlCountPerInnerLoop)
                        + ", thread count: " + threadCount + ", avg time: " + avgTime + " ms"
                        + ", total time: " + totalTime + " ms" + (warmUp ? " (***WarmUp***)" : ""));
    }

    protected long toMillis(long duration) {
        return TimeUnit.NANOSECONDS.toMillis(duration);
    }

    protected ClientServerBTestThread createBTestThread(int id, Connection conn) {
        throw new RuntimeException("not supports");
    }

    protected abstract class ClientServerBTestThread extends Thread {

        protected Connection conn;
        protected Statement stmt;
        protected PreparedStatement ps;
        protected boolean closeConn = true;
        protected long startTime;
        protected long endTime;
        protected long t1;
        protected long t2;

        public ClientServerBTestThread(int id, Connection conn) {
            super(getBTestName() + "Thread-" + id);
            t1 = System.currentTimeMillis();
            this.conn = conn;
            try {
                this.stmt = conn.createStatement();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        public long getStartTime() {
            return startTime;
        }

        public long getEndTime() {
            return endTime;
        }

        public long getTotalTime() {
            return endTime - startTime;
        }

        public void setCloseConn(boolean closeConn) {
            this.closeConn = closeConn;
        }

        public void prepareStatement(String sql) {
            try {
                ps = conn.prepareStatement(sql);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        protected abstract String nextSql();

        protected void prepare() throws Exception {
        }

        protected void execute() throws Exception {
        }

        @Override
        public void run() {
            try {
                execute();
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                t2 = System.currentTimeMillis();
                close(stmt, ps);
                if (closeConn)
                    close(conn);
            }
        }
    }

    protected Connection getConnection() throws Exception {
        switch (dbType) {
        case H2:
            return embedded ? getEmbeddedH2Connection() : getH2Connection();
        case MYSQL:
            return getMySQLConnection();
        case POSTGRESQL:
            return getPgConnection();
        case LEALONE: {
            Connection conn = embedded ? getEmbeddedLealoneConnection() : getLealoneConnection(async);
            if (disableLealoneQueryCache) {
                disableLealoneQueryCache(conn);
            }
            return conn;
        }
        case SQLITE:
            return getSQLiteConnection();
        case LM: {
            Connection conn = getLMConnection();
            if (disableLealoneQueryCache) {
                disableLealoneQueryCache(conn);
            }
            return conn;
        }
        default:
            throw new RuntimeException();
        }
    }

    protected String getBTestName() {
        return getClass().getSimpleName();
    }

    protected static void close(AutoCloseable... acArray) {
        for (AutoCloseable ac : acArray) {
            if (ac != null) {
                try {
                    ac.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static Connection getLMConnection() throws Exception {
        String db = "mysql";
        String user = "root";
        String password = "";
        int port = 9310;
        return getMySQLConnection(db, user, password, port);
    }

    public static Connection getMySQLConnection() throws Exception {
        String db = "test";
        String user = "test";
        String password = "test";
        int port = 3306;
        return getMySQLConnection(db, user, password, port);
    }

    public static Connection getMySQLConnection(String db, String user, String password, int port)
            throws Exception {
        String url = "jdbc:mysql://localhost:" + port + "/" + db;

        Properties info = new Properties();
        info.put("user", user);
        info.put("password", password);
        // info.put("holdResultsOpenOverStatementClose","true");
        // info.put("allowMultiQueries","true");

        info.put("useServerPrepStmts", "true");
        info.put("cachePrepStmts", "true");
        info.put("rewriteBatchedStatements", "true");
        // info.put("useCompression", "true");
        info.put("serverTimezone", "GMT");

        Connection conn = DriverManager.getConnection(url, info);
        // conn.setAutoCommit(true);
        return conn;
    }

    public static Connection getPgConnection() throws Exception {
        String url = "jdbc:postgresql://localhost:" + 5432 + "/test";
        return getConnection(url, "test", "test");
    }

    public static Connection getH2Connection() throws Exception {
        String url = "jdbc:h2:tcp://localhost:9092/mydb";
        return getConnection(url, "sa", "");
    }

    public static Connection getEmbeddedH2Connection() throws Exception {
        String url;
        url = "jdbc:h2:file:" + BENCH_TEST_BASE_DIR + "/h2/EmbeddedBenchTestDB";
        // url = "jdbc:h2:mem:mydb";
        // url += ";OPEN_NEW=true;FORBID_CREATION=false";
        url += ";FORBID_CREATION=false";
        return DriverManager.getConnection(url, "sa", "");
    }

    public static Connection getSQLiteConnection() throws Exception {
        File path = new File(BENCH_TEST_BASE_DIR + "/sqlite");
        if (!path.exists())
            path.mkdirs();
        String url = "jdbc:sqlite:" + path.getCanonicalPath() + "/EmbeddedBenchTestDB.db";
        Properties info = new Properties();
        info.put("journal_mode", "WAL");
        // info.put("journal_mode", "OFF");
        // info.put("journal_mode", "MEMORY");
        info.put("synchronous", "NORMAL"); // 支持多线程写

        SQLiteConfig config = new SQLiteConfig();
        config.setSharedCache(true); // 关闭共享缓存（MULTITHREADED模式建议关闭）
        config.setJournalMode(SQLiteConfig.JournalMode.WAL);
        config.setSynchronous(SQLiteConfig.SynchronousMode.NORMAL);
        info = config.toProperties();
        return DriverManager.getConnection(url, info);
    }

    public static String getLealoneUrl() {
        String url = "jdbc:lealone:tcp://localhost:" + Constants.DEFAULT_TCP_PORT + "/lealone";
        url += "?" + ConnectionSetting.NETWORK_TIMEOUT + "=" + Integer.MAX_VALUE;
        return url;
    }

    public static Connection getEmbeddedLealoneConnection() throws Exception {
        SysProperties.setBaseDir(joinDirs("lealone"));
        String url = "jdbc:lealone:embed:EmbeddedBenchTestDB?" + DbSetting.PERSISTENT
                + "=true&ANALYZE_AUTO=0";
        return DriverManager.getConnection(url, "root", "");
    }

    public static Connection getLealoneConnection(boolean async) throws Exception {
        String url = getLealoneUrl();
        url += "&" + ConnectionSetting.IS_SHARED + "=false";
        // url += "&" + ConnectionSetting.SCHEDULER_COUNT + "=16";
        url += "&" + ConnectionSetting.NET_FACTORY_NAME + "=" + (async ? "nio" : "bio");
        return getConnection(url, "root", "");
    }

    public static Connection getLealoneSharedConnection(int maxSharedSize) throws Exception {
        String url = getLealoneUrl();
        url += "&" + ConnectionSetting.IS_SHARED + "=true";
        url += "&" + ConnectionSetting.MAX_SHARED_SIZE + "=" + maxSharedSize;
        return getConnection(url, "root", "");
    }

    public static void disableLealoneQueryCache(Connection conn) {
        try {
            Statement statement = conn.createStatement();
            statement.executeUpdate("set QUERY_CACHE_SIZE 0");
            // statement.executeUpdate("set ANALYZE_AUTO 0");
            // statement.executeUpdate("set OPTIMIZE_REUSE_RESULTS 0");
            statement.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Connection getConnection(String url, String user, String password) throws Exception {
        Properties info = new Properties();
        info.put("user", user);
        info.put("password", password);
        return DriverManager.getConnection(url, info);
    }
}
