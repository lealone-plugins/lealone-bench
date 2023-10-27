/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.bench.embed.storage;

import java.util.concurrent.ConcurrentSkipListMap;

public class SkipListBTest extends StorageMapBTest {

    public static void main(String[] args) throws Exception {
        new SkipListBTest().run();
    }

    private ConcurrentSkipListMap<Integer, String> map;

    @Override
    protected void init() {
        if (!inited.compareAndSet(false, true))
            return;
        map = new ConcurrentSkipListMap<>();
    }

    @Override
    protected void createData() {
        // map.clear();
        if (map.isEmpty())
            singleThreadSerialWrite();
    }

    @Override
    protected int size() {
        return map.size();
    }

    @Override
    protected void put(Integer key, String value) {
        map.put(key, value);
    }

    @Override
    protected String get(Integer key) {
        return map.get(key);
    }
}
