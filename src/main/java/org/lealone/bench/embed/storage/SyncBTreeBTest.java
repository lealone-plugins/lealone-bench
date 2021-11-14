/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.bench.embed.storage;

import org.lealone.db.value.ValueInt;
import org.lealone.db.value.ValueString;

public class SyncBTreeBTest extends StorageMapBTest {

    public static void main(String[] args) throws Exception {
        new SyncBTreeBTest().run();
    }

    @Override
    protected void init() {
        if (!inited.compareAndSet(false, true))
            return;
        initConfig();
        openStorage(false);
        openMap();
    }

    @Override
    protected void openMap() {
        if (map == null || map.isClosed()) {
            map = storage.openBTreeMap(SyncBTreeBTest.class.getSimpleName(), ValueInt.type, ValueString.type, null);
        }
    }
}
