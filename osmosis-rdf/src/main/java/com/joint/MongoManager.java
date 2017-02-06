package com.joint;

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
public class MongoManager {

    private MongoClient mongo = new MongoClient("localhost");
    private MongoDatabase mongo_DB;

    public MongoManager() {
        mongo_DB = mongo.getDatabase("main");
    }

    public Document contains(double lon, double lat) {

        Point point = new Point(new Position(lon, lat));
        System.out.println(Filters.geoIntersects("loc", point));
        return mongo_DB.getCollection("city").find(Filters.geoIntersects("loc", point)).projection(Projections.include("dataset", "@id")).first();
    }

    public static void main(String[] args) {
        MongoManager mongoManager = new MongoManager();
        System.out.println(mongoManager.contains(-46.6618907, -23.5281847));
    }
}
