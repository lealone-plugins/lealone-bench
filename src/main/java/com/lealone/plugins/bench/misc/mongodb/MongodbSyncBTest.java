/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.bench.misc.mongodb;

import org.bson.Document;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public abstract class MongodbSyncBTest extends DocDatabaseBTest {

    MongoClient[] mongoClients;

    @Override
    void createMongoClients(int port) {
        String connectionString = getConnectionString(port);
        mongoClients = new MongoClient[clientCount];
        for (int i = 0; i < clientCount; i++) {
            mongoClients[i] = MongoClients.create(connectionString);
        }
    }

    @Override
    void closeMongoClients() {
        for (int i = 0; i < clientCount; i++) {
            mongoClients[i].close();
        }
    }

    MongoCollection<Document> getCollection(int clientIndex) {
        MongoClient mongoClient = mongoClients[clientIndex % clientCount];
        MongoDatabase database = mongoClient.getDatabase("test");
        return database.getCollection(getClass().getSimpleName());
    }

    @Override
    long benchTest(int clientIndex) {
        MongoCollection<Document> collection = getCollection(clientIndex);
        long t1 = System.nanoTime();
        for (int j = 0; j < innerLoop; j++) {
            execute(collection);
        }
        long t2 = System.nanoTime();
        return t2 - t1;
    }

    abstract void execute(MongoCollection<Document> collection);
}
