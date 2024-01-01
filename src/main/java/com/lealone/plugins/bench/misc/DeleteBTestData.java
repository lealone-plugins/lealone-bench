/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.bench.misc;

import java.io.File;
import java.io.IOException;

import com.lealone.storage.fs.FileUtils;

import com.lealone.plugins.bench.BenchTest;

public class DeleteBTestData {

    public static void main(String[] args) throws IOException {
        FileUtils.deleteRecursive(BenchTest.BENCH_TEST_BASE_DIR, true);
        if (!FileUtils.exists(BenchTest.BENCH_TEST_BASE_DIR)) {
            System.out.println(
                    "dir '" + new File(BenchTest.BENCH_TEST_BASE_DIR).getCanonicalPath() + "' deleted");
        }
    }
}
