/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.bench.embed.storage;

import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;

public class H2MVMapBTest extends StorageMapBTest {

    public static void main(String[] args) throws Exception {
        new H2MVMapBTest().run();
    }

    MVMap<Integer, String> map;

    @Override
    protected void init() {
        if (!inited.compareAndSet(false, true))
            return;
        MVStore store = MVStore.open(null);
        map = store.openMap(H2MVMapBTest.class.getSimpleName());
    }

    @Override
    protected void beforeRun() {
        map.clear();
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
