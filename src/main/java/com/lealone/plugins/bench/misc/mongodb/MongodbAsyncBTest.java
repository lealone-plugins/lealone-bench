/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.bench.misc.mongodb;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.bson.Document;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.mongodb.client.result.InsertOneResult;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;

public abstract class MongodbAsyncBTest extends DocDatabaseBTest {

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
        return database.getCollection("test");
    }

    long countDocuments() {
        AtomicLong count = new AtomicLong();
        MongoCollection<Document> collection = getCollection(0);
        CountDownLatch latch = new CountDownLatch(1);
        collection.countDocuments().subscribe(new Subscriber<Long>() {
            @Override
            public void onSubscribe(Subscription s) {
                s.request(1);
            }

            @Override
            public void onNext(Long t) {
                count.set(t);
            }

            @Override
            public void onError(Throwable t) {
                latch.countDown();
            }

            @Override
            public void onComplete() {
                latch.countDown();
            }
        });
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return count.get();
    }

    @Override
    long benchTest(int clientIndex) {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger counter = new AtomicInteger(innerLoop);
        MongoCollection<Document> collection = getCollection(clientIndex);
        long t1 = System.nanoTime();
        for (int j = 0; j < innerLoop; j++) {
            execute(collection, latch, counter);
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        long t2 = System.nanoTime();
        return t2 - t1;
    }

    abstract void execute(MongoCollection<Document> collection, CountDownLatch latch,
            AtomicInteger counter);

    static void insertOne(MongoCollection<Document> collection, CountDownLatch latch,
            AtomicInteger counter, Document doc) {
        Publisher<InsertOneResult> publisher = collection.insertOne(doc);
        publisher.subscribe(new Subscriber<InsertOneResult>() {
            @Override
            public void onSubscribe(Subscription s) {
                s.request(1);
            }

            @Override
            public void onNext(InsertOneResult t) {
            }

            @Override
            public void onError(Throwable t) {
                t.printStackTrace();
            }

            @Override
            public void onComplete() {
                if (counter.decrementAndGet() == 0) {
                    latch.countDown();
                }
            }
        });
    }
}
