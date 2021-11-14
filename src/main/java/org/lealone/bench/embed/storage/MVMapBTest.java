/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.bench.embed.storage;

//import org.h2.mvstore.MVStore;
////import org.lealone.plugins.mvstore.MVStorage;
//import org.lealone.storage.type.ObjectDataType;

public class MVMapBTest extends StorageMapBTest {

    public static void main(String[] args) throws Exception {
        new MVMapBTest().run();
    }

    @Override
    protected void init() {
        // MVStore.Builder builder = new MVStore.Builder();
        // MVStore store = MVStore.open(null);

        // MVStorage mvs = new MVStorage(store, null);
        // map = mvs.openMap(MVMapPerfTest.class.getSimpleName(), new ObjectDataType(), new ObjectDataType(), null);
    }
}
