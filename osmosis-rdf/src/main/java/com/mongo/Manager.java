package com.mongo;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.geojson.Point;
import com.mongodb.client.model.geojson.Position;
import org.bson.Document;

/**
 *
 * @author williams
 */
public class Manager {

    private MongoClient mongo = new MongoClient("localhost");
    private MongoDatabase mongo_DB;

    public Manager() {
        mongo_DB = mongo.getDatabase("main");
    }

    public Document get(long id) {
        return mongo_DB.getCollection("node").find(Filters.eq("_id", id)).projection(Projections.include("dataset", "city")).first();
    }

    public Document contains(double lon, double lat) {

        Point point = new Point(new Position(lon, lat));
        System.out.println(Filters.geoIntersects("loc", point));
        return mongo_DB.getCollection("city").find(Filters.geoIntersects("loc", point)).projection(Projections.include("dataset", "@id")).first();
    }
}
