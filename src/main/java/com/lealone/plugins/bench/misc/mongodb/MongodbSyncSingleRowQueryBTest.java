/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.bench.misc.mongodb;

import org.bson.Document;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;

public class MongodbSyncSingleRowQueryBTest extends MongodbSyncBTest {

    public static void main(String[] args) {
        new MongodbSyncSingleRowQueryBTest().run(MONGODB_PORT);
    }

    @Override
    void beforeBenchTest() {
        operation = "query";
        MongoCollection<Document> collection = getCollection(0);
        // collection.drop();
        if (collection.countDocuments() <= 0)
            insert(collection);
        // query(collection);
    }

    void insert(MongoCollection<Document> collection) {
        for (int i = 0; i < rowCount; i++) {
            Document doc1 = new Document("_id", id.incrementAndGet()).append("f1", i);
            collection.insertOne(doc1);
        }
        System.out.println("Total document count: " + collection.countDocuments());
    }

    void query(MongoCollection<Document> collection) {
        MongoCursor<Document> cursor = collection.find(Filters.eq("_id", 1)).iterator();
        try {
            while (cursor.hasNext()) {
                System.out.println(cursor.next().toJson());
            }
        } finally {
            cursor.close();
        }
    }

    @Override
    void execute(MongoCollection<Document> collection) {
        // 用f1查MongoDB很慢
        String key = "f1";
        key = "_id";
        MongoCursor<Document> cursor = collection.find(Filters.eq(key, random.nextInt(rowCount)))
                .iterator();
        try {
            while (cursor.hasNext()) {
                cursor.next();
                break;
            }
        } finally {
            cursor.close();
        }
    }
}
