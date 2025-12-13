/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.bench.cs.write.append;

public class LealoneAppendBTest extends AppendBTest {

    public static void main(String[] args) {
        new LealoneAppendBTest().start();
        // System.out.println("jvm pid: "
        // + java.lang.management.ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);
        // try {
        // Thread.sleep(9000 * 1000);
        // } catch (InterruptedException e) {
        // e.printStackTrace();
        // }
    }
}
