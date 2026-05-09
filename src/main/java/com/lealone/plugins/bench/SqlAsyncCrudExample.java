package com.lealone.plugins.bench;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import com.mysql.cj.xdevapi.Result;
import com.mysql.cj.xdevapi.Row;
import com.mysql.cj.xdevapi.RowResult;
import com.mysql.cj.xdevapi.Session;
import com.mysql.cj.xdevapi.SessionFactory;
import com.mysql.cj.xdevapi.SqlResult;

public class SqlAsyncCrudExample {

    // 连接字符串，端口必须是 X Protocol 的 33060
    private static final String CONNECTION_URI = "mysqlx://test:test@localhost:33060/test";

    public static void main(String[] args) {
        try (Session session = new SessionFactory().getSession(CONNECTION_URI)) {
            System.out.println("会话已创建，连接成功！");

            // --- 1. 异步建表 (Create Table) ---
            System.out.println("\n--- 开始异步建表 ---");
            String createTableSql = "CREATE TABLE IF NOT EXISTS employees ("
                    + "id INT AUTO_INCREMENT PRIMARY KEY, " + "name VARCHAR(50), " + "age INT)";

            // 使用 session.sql() 执行 DDL
            CompletableFuture<SqlResult> createTableFuture = session.sql(createTableSql).executeAsync();
            createTableFuture.get(); // 等待完成
            System.out.println("表 'employees' 准备就绪。");

            // --- 2. 异步插入数据 (Create) ---
            System.out.println("\n--- 开始异步插入数据 ---");
            // 使用占位符 :name 和 :age 防止 SQL 注入
            String insertSql = "INSERT INTO employees (name, age) VALUES (?, ?)";

            CompletableFuture<SqlResult> insertFuture = session.sql(insertSql).bind("Alice").bind(30)
                    .executeAsync();

            SqlResult insertResult = insertFuture.get();
            System.out.println("插入成功，受影响行数: " + insertResult.getAffectedItemsCount());
            System.out.println("生成的自增ID: " + insertResult.getAutoIncrementValue());

            // --- 3. 异步查询数据 (Read) ---
            System.out.println("\n--- 开始异步查询数据 ---");
            String selectSql = "SELECT id, name, age FROM employees WHERE age > ?";

            CompletableFuture<SqlResult> selectFuture = session.sql(selectSql).bind(20).executeAsync();

            // 等待查询结果
            RowResult rowResult = selectFuture.get();

            System.out.println("查询结果:");
            // 遍历结果集
            for (Row row : rowResult) {
                System.out.println("ID: " + row.getInt("id") + ", Name: " + row.getString("name")
                        + ", Age: " + row.getInt("age"));
            }

            // --- 4. 异步更新数据 (Update) ---
            System.out.println("\n--- 开始异步更新数据 ---");
            String updateSql = "UPDATE employees SET age = ? WHERE name = ?";

            CompletableFuture<SqlResult> updateFuture = session.sql(updateSql).bind(31).bind("Alice")
                    .executeAsync();

            Result updateResult = updateFuture.get();
            System.out.println("更新成功，受影响行数: " + updateResult.getAffectedItemsCount());

            // --- 5. 异步删除数据 (Delete) ---
            System.out.println("\n--- 开始异步删除数据 ---");
            String deleteSql = "DELETE FROM employees WHERE name = ?";

            CompletableFuture<SqlResult> deleteFuture = session.sql(deleteSql).bind("Alice")
                    .executeAsync();

            Result deleteResult = deleteFuture.get();
            System.out.println("删除成功，受影响行数: " + deleteResult.getAffectedItemsCount());

        } catch (ExecutionException | InterruptedException e) {
            System.err.println("异步操作执行出错: " + e.getMessage());
            e.printStackTrace();
        } catch (Exception e) {
            System.err.println("发生其他错误: " + e.getMessage());
            e.printStackTrace();
        }
        System.out.println("\n所有操作完成。");
    }
}