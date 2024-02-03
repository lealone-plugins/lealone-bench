/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.bench.misc.mongodb;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.client.model.Filters;

public abstract class DocDatabaseBTest {

    public static final int LEALONE_PORT = 9310;
    public static final int MONGODB_PORT = 27017;

    int threadCount = 48;
    int outerLoop = 30;
    int innerLoop = 200;

    int clientCount = 2; // 超过cpu核数性能会下降

    void beforeBenchTest() {
    }

    void afterBenchTest() {
    }

    public static void testDocument() {
        Document doc = Document.parse("{ status: { $in: [ \"A\", \"D\" ] } }");
        System.out.println(doc.toJson());

        Bson bson = Filters.and(Filters.eq("_id", 1), Filters.eq("_id", 2));
        System.out.println(bson.toBsonDocument().toJson());

        bson = Filters.in("_id", 1, 2, 2);
        System.out.println(bson.toBsonDocument().toJson());
    }
}
