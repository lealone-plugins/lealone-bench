/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.bench.misc;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.HashMap;

import com.lealone.plugins.bench.BenchTest;
import com.lealone.storage.fs.FileStorage;
import com.lealone.storage.fs.FileUtils;

public class FsyncTest {

    public static void main(String[] args) {
        int size = 4 * 1024;
        ByteBuffer buff = ByteBuffer.allocateDirect(size);
        for (int i = 0; i < size; i++)
            buff.put((byte) i);
        buff.flip();
        String name = BenchTest.BENCH_TEST_BASE_DIR + "/FsyncTest.db";
        // name = "F:/" + name;
        new File(name).mkdirs();
        FileUtils.delete(name);
        FileStorage file = FileStorage.open(name, new HashMap<>());

        for (int i = 0; i < 50; i++) {
            long t1 = System.nanoTime();
            file.writeFully(i * size, buff);
            long t2 = System.nanoTime();
            buff.flip();

            long t3 = System.nanoTime();
            file.sync();
            long t4 = System.nanoTime();
            System.out.println("write " + size + " bytes, write time: " + (t2 - t1) / 1000
                    + " μs, fsync time: " + (t4 - t3) / 1000 / 1000 + " ms");
        }
        file.close();
    }
}
