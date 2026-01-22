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
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.sqlite.SQLiteConfig;

import com.lealone.db.ConnectionSetting;
import com.lealone.db.Constants;
import com.lealone.db.DbSetting;
import com.lealone.db.SysProperties;
import com.lealone.db.async.AsyncTask;
import com.lealone.db.scheduler.SchedulerFactoryBase;
import com.lealone.plugins.bench.BenchTest;
import com.lealone.plugins.bench.DbType;

public abstract class ClientServerBTest extends BenchTest {

    static {
        System.setProperty("lealone.server.cached.objects", "10000000");
        System.setProperty("h2.serverCachedObjects", "10000000");
    }

    protected DbType dbType;
    protected boolean disableLealoneQueryCache = true;

    protected int benchTestLoop;
    protected int outerLoop;
    protected int innerLoop;
    protected int sqlCountPerInnerLoop;
    protected boolean printInnerLoopResult;
    protected boolean async;
    protected boolean autoCommit = true;
    protected boolean batch;
    protected boolean prepare;
    protected boolean reinit;
    protected String[] sqls;

    protected AtomicInteger id = new AtomicInteger();
    protected Random random = new Random();

    protected ExecutorService executorService;

    protected boolean embedded;
    protected boolean useVirtualThread;

    protected int connCount;

    public ClientServerBTest() {
        benchTestLoop = 30;
        outerLoop = 15;
        innerLoop = 10;
        sqlCountPerInnerLoop = 10;

        threadCount = 16;
        // connCount = 160;

        reinit = false;

        // prepare = true;
        // embedded = true;

        // useVirtualThread = true;
    }

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
        if (useVirtualThread) {
            // 这种方式遇到阻塞io会挂起平台线程
            // executorService = Executors.newFixedThreadPool(1, factory);
            ThreadFactory factory = Thread.ofVirtual().name("vt-", 1).factory();
            executorService = Executors.newThreadPerTaskExecutor(factory);
        }

        if (connCount == 0)
            connCount = useVirtualThread ? threadCount * 1 : threadCount;
        Connection[] conns = new Connection[connCount];
        for (int i = 0; i < connCount; i++) {
            conns[i] = getConnection();
        }
        if (disableLealoneQueryCache) {
            switch (dbType) {
            case LEALONE:
            case LM:
            case LP:
                for (int i = 0; i < connCount; i++) {
                    disableLealoneQueryCache(conns[i]);
                }
                break;
            }
        }

        for (int i = 0; i < benchTestLoop; i++) {
            if (reinit || i == 0) {
                id.set(0);
                init();
            }
            runOuterLoop(threadCount, conns);
        }
        for (int i = 0; i < connCount; i++) {
            close(conns[i]);
        }

        if (useVirtualThread) {
            executorService.shutdown();
        }
    }

    protected void runOuterLoop(int threadCount, Connection[] conns) throws Exception {
        long t1 = System.currentTimeMillis();
        for (int i = 0; i < outerLoop; i++) {
            run(threadCount, conns);
        }
        long t2 = System.currentTimeMillis();
        System.out.println(getBTestName() + " sql count: "
                + (outerLoop * threadCount * innerLoop * sqlCountPerInnerLoop) + ", total time: "
                + (t2 - t1) + " ms");
    }

    protected final static LinkedBlockingQueue<Statement> stmtQueue = new LinkedBlockingQueue<>();
    protected static Statement[] stmtArray;
    protected static final AtomicInteger stmtIndex = new AtomicInteger(0);

    public static Statement nextStatement() {
        return stmtArray[SchedulerFactoryBase.getAndIncrementIndex(stmtIndex) % stmtArray.length];
    }

    protected void run(int threadCount, Connection[] conns) throws Exception {
        ClientServerBTestThread[] threads = new ClientServerBTestThread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            threads[i] = createBTestThread();
            threads[i].init(i, conns[i]);
        }
        if (useVirtualThread) {
            stmtQueue.clear();
            stmtArray = new Statement[conns.length];
            for (int i = 0; i < conns.length; i++) {
                Statement stmt = conns[i].createStatement();
                stmtQueue.add(stmt);
                stmtArray[i] = stmt;
            }
            // JdbcStatement.stmtQueue = stmtQueue;
        }
        long t1 = System.currentTimeMillis();
        for (int i = 0; i < threadCount; i++) {
            ClientServerBTestThread thread = threads[i];
            if (useVirtualThread)
                executorService.submit(thread);
            else
                threads[i].start();
        }
        long totalTime = 0;
        for (int i = 0; i < threadCount; i++) {
            threads[i].await();
            totalTime += threads[i].getTotalTime();
        }
        long t2 = System.currentTimeMillis();
        long avgTime = toMillis(totalTime / threadCount);
        totalTime = (t2 - t1);
        System.out.println(
                getBTestName() + " sql count: " + (threadCount * innerLoop * sqlCountPerInnerLoop) + ", "
                        + (useVirtualThread ? "v" : "") + "thread count: " + threadCount + ", avg time: "
                        + avgTime + " ms" + ", total time: " + totalTime + " ms");

        for (int i = 0; i < threadCount; i++) {
            threads[i].end();
        }
    }

    protected long toMillis(long duration) {
        return TimeUnit.NANOSECONDS.toMillis(duration);
    }

    protected ClientServerBTestThread createBTestThread() {
        throw new RuntimeException("not supports");
    }

    protected boolean isQuery() {
        return false;
    }

    protected abstract class ClientServerBTestThread extends Thread implements AsyncTask {

        protected final Random random = new Random();
        protected final CountDownLatch latch = new CountDownLatch(1);
        protected final AtomicInteger innerLoopRemaining = new AtomicInteger(
                innerLoop * sqlCountPerInnerLoop);

        protected Connection conn;
        protected Statement stmt;
        protected PreparedStatement ps;
        protected boolean closeConn;
        protected long startTime;
        protected long endTime;

        public void init(int id, Connection conn) {
            setName(getBTestName() + "Thread-" + id);
            this.conn = conn;
            try {
                this.stmt = conn.createStatement();
                String sql = prepareSql();
                if (sql != null)
                    prepareStatement(sql);
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

        public void prepareStatement(String sql) {
            try {
                if (prepare)
                    ps = conn.prepareStatement(sql);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        protected abstract String nextSql();

        protected String prepareSql() {
            return null;
        }

        protected void prepare() throws Exception {
        }

        protected void execute() throws Exception {
        }

        @Override
        public void run() {
            startTime = System.nanoTime();
            try {
                if (!autoCommit)
                    conn.setAutoCommit(false);
                execute();
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                if (!useVirtualThread && !async) {
                    onComplete();
                }
            }
        }

        protected void onComplete() {
            if (!autoCommit) {
                try {
                    conn.commit();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
            endTime = System.nanoTime();
            latch.countDown();
        }

        protected void onComplete(int c) {
            if (innerLoopRemaining.addAndGet(-c) == 0) {
                onComplete();
            }
        }

        public void await() {
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        public void end() {
            close(stmt, ps);
            if (closeConn)
                close(conn);
        }

        protected void printInnerLoopResult(long t1) {
            if (printInnerLoopResult) {
                long t2 = System.nanoTime();
                System.out.println(getBTestName() + " sql count: " + (innerLoop * sqlCountPerInnerLoop) //
                        + " total time: " + toMillis(t2 - t1) + " ms");
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
        case SQLITE:
            return getSQLiteConnection();
        case LEALONE:
            return embedded ? getEmbeddedLealoneConnection(threadCount)
                    : getLealoneConnection(async, useVirtualThread, threadCount, connCount, isQuery());
        case LM:
            return getLMConnection();
        case LP:
            return getLPConnection();
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
        info.put("cachePrepStmts", "false"); // 如果为true比直接拼装sql执行还慢
        info.put("rewriteBatchedStatements", "true");
        // info.put("useCompression", "true");
        info.put("serverTimezone", "GMT");

        Connection conn = DriverManager.getConnection(url, info);
        // conn.setAutoCommit(true);
        return conn;
    }

    public static Connection getLPConnection() throws Exception {
        String url = "jdbc:postgresql://localhost:" + 9510 + "/postgres";

        Properties info = new Properties();
        info.put("user", "postgres");
        info.put("password", "postgres");

        return DriverManager.getConnection(url, info);
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
        // config.setSharedCache(false);
        config.setJournalMode(SQLiteConfig.JournalMode.WAL);
        config.setSynchronous(SQLiteConfig.SynchronousMode.NORMAL);
        info = config.toProperties();
        return DriverManager.getConnection(url, info);
    }

    public static String getLealoneUrl() {
        String url = "jdbc:lealone:tcp://localhost:" + Constants.DEFAULT_TCP_PORT + "/lealone";
        url += "?" + ConnectionSetting.NETWORK_TIMEOUT + "=" + Integer.MAX_VALUE;
        url += "&ANALYZE_AUTO=0";
        return url;
    }

    public static Connection getEmbeddedLealoneConnection(int threadCount) throws Exception {
        SysProperties.setBaseDir(joinDirs("lealone"));
        threadCount = Runtime.getRuntime().availableProcessors();
        String url = "jdbc:lealone:embed:EmbeddedBenchTestDB?" + DbSetting.PERSISTENT
        // + "=true&ANALYZE_AUTO=0";
                + "=true&ANALYZE_AUTO=0&SCHEDULER_COUNT=" + threadCount;
        return DriverManager.getConnection(url, "root", "");
    }

    public static Connection getLealoneConnection(boolean async, boolean useVirtualThread,
            int threadCount, int connCount, boolean isQuery) throws Exception {
        if (useVirtualThread) {
            if (isQuery)
                threadCount = threadCount * 1; // 查询场景，线程数增加一倍更优
            else
                threadCount = 2; // 更新场景，线程数是2更优
        } else {
            // 调度器线程的数量默认就是cpu核数，默认就是最佳的
            // async = true;
            threadCount = Runtime.getRuntime().availableProcessors();
            threadCount = threadCount * 1;
        }
        String url = getLealoneUrl();
        if (async || useVirtualThread) {
            url += "&" + ConnectionSetting.IS_SHARED + "=false";
            int maxSharedSize = (connCount / threadCount);
            // maxSharedSize = 1;
            if (maxSharedSize > 0)
                url += "&" + ConnectionSetting.MAX_SHARED_SIZE + "=" + maxSharedSize;
            url += "&" + ConnectionSetting.SCHEDULER_COUNT + "=" + threadCount;
            url += "&" + ConnectionSetting.MAX_PACKET_COUNT_PER_LOOP + "=50";
        }
        url += "&" + ConnectionSetting.NET_FACTORY_NAME + "=" + (async ? "nio" : "bio");
        return getConnection(url, "root", "");
    }

    public static Connection getLealoneSharedConnection(int maxSharedSize) throws Exception {
        String url = getLealoneUrl();
        url += "&" + ConnectionSetting.IS_SHARED + "=true";
        url += "&" + ConnectionSetting.MAX_SHARED_SIZE + "=" + maxSharedSize;
        url += "&" + ConnectionSetting.NET_FACTORY_NAME + "=" + "nio";
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
