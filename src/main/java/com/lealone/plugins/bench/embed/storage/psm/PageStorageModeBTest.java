/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.bench.embed.storage.psm;

import java.util.HashMap;

import com.lealone.db.index.standard.PrimaryKeyType;
import com.lealone.db.row.Row;
import com.lealone.db.row.RowType;
import com.lealone.db.value.Value;
import com.lealone.db.value.ValueLong;
import com.lealone.db.value.ValueString;
import com.lealone.storage.CursorParameters;
import com.lealone.storage.StorageMap;
import com.lealone.storage.StorageMapCursor;
import com.lealone.storage.StorageSetting;
import com.lealone.storage.aose.AOStorage;
import com.lealone.storage.aose.btree.BTreeMap;
import com.lealone.storage.aose.btree.page.PageStorageMode;
import com.lealone.test.TestBase;
import com.lealone.test.aose.AoseTestBase;

//把CACHE_SIZE加大后，RowStorage的方式有更多内存就不会重复从硬盘读取page，此时就跟ColumnStorage的性能差不多
public class PageStorageModeBTest extends TestBase {

    public static void main(String[] args) throws Exception {
        new PageStorageModeBTest().run();
    }

    int rowCount = 6000;
    int columnCount = 10;
    int pageSize = 1024 * 1024;
    int cacheSize = 100 * 1024 * 1024; // 100M

    public void run() {
        PrimaryKeyType keyType = new PrimaryKeyType();
        RowType valueType = new RowType(null, columnCount);

        for (int i = 0; i < 10; i++) {
            System.out.println();
            System.out.println("------------------loop " + (i + 1) + " start---------------------");
            testRowStorage(keyType, valueType);

            System.out.println();
            testColumnStorage(keyType, valueType);
            System.out.println("------------------loop " + (i + 1) + " end---------------------");
        }
    }

    private void putData(StorageMap<ValueLong, Row> map) {
        if (!map.isEmpty())
            return;
        for (int row = 1; row <= rowCount; row++) {
            ValueLong key = ValueLong.get(row);
            Value[] columns = new Value[columnCount];
            for (int col = 0; col < columnCount; col++) {
                columns[col] = ValueString.get("value-row" + row + "-col" + (col + 1));
            }
            Row r = new Row(row, columns);
            map.put(key, r);
        }
        map.save();
    }

    private void testRowStorage(PrimaryKeyType keyType, RowType valueType) {
        testStorage(keyType, valueType, PageStorageMode.ROW_STORAGE, "testRowStorage");
    }

    private void testColumnStorage(PrimaryKeyType keyType, RowType valueType) {
        testStorage(keyType, valueType, PageStorageMode.COLUMN_STORAGE, "testColumnStorage");
    }

    private void testStorage(PrimaryKeyType keyType, RowType valueType, PageStorageMode mode,
            String mapName) {
        AOStorage storage = AoseTestBase.openStorage(pageSize, cacheSize);
        HashMap<String, String> parameters = new HashMap<>();
        parameters.put(StorageSetting.PAGE_STORAGE_MODE.name(), mode.name());
        BTreeMap<ValueLong, Row> map = storage.openBTreeMap(mapName, keyType, valueType, parameters);
        putData(map);

        ValueLong firstKey = map.firstKey();
        assertEquals(1, firstKey.getLong());

        int columnIndex = 2; // 索引要从0开始算

        ValueLong key = ValueLong.get(4000);
        Row r = map.get(key);
        Value columnValue = r.getColumns()[columnIndex];
        assertEquals("value-row4000-col3", columnValue.getString());

        key = ValueLong.get(2);
        r = map.get(key, columnIndex);
        columnValue = r.getColumns()[columnIndex];
        assertEquals("value-row2-col3", columnValue.getString());

        key = ValueLong.get(2999);
        r = map.get(key, columnIndex);
        columnValue = r.getColumns()[columnIndex];
        assertEquals("value-row2999-col3", columnValue.getString());

        int rows = 0;
        ValueLong from = ValueLong.get(2000);
        StorageMapCursor<ValueLong, Row> cursor = map.cursor(CursorParameters.create(from, columnIndex));
        while (cursor.next()) {
            rows++;
        }
        assertEquals(rowCount - 2000 + 1, rows);
        map.close();
    }
}
