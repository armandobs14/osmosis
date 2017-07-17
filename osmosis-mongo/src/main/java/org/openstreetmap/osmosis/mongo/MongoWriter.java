package org.openstreetmap.osmosis.mongo;

import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.geojson.*;
import org.bson.Document;
import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.*;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;
import java.util.*;
import java.util.logging.Logger;

/**
 * Created by armando on 10/10/16.
 */
public class MongoWriter implements Sink {

    Logger logger = Logger.getLogger(MongoWriter.class.getName());
    private String host;
    private MongoClient client;
    private MongoDatabase db;

    public MongoWriter(MongoClient client) {
        db = client.getDatabase("main");
        db.getCollection("node").createIndex(new BasicDBObject("loc", "2dsphere"));
    }

    public void process(EntityContainer entityContainer) {

        Entity entity = entityContainer.getEntity();
        EntityType type = entity.getType();
        MongoCollection<Document> nodeCollection = db.getCollection("node");

        if (entity.getType().equals(EntityType.Node)) {
            Node node = (Node) entity;

            TagCollection tagCollection = (TagCollection) node.getTags();
            Map<String, Object> map = new HashMap<>();
//            Map<String,Object> point = new HashMap<>();
            tagCollection.buildMap().forEach((k, v) -> {
                map.put(k.replace(".", "_"), v);
            });

            Point point = new Point(new Position(node.getLongitude(), node.getLatitude()));
            map.put("loc", point);

            //metadata
            map.put("_id", entity.getId());
            map.put("changesetId", entity.getChangesetId());
            map.put("user_id", entity.getUser().getId());
            map.put("user_name", entity.getUser().getName());
            map.put("hashCode", entity.hashCode());
            map.put("version", entity.getVersion());
            map.put("timestamps", entity.getTimestamp().getTime());

            Document doc = new Document(map);
            nodeCollection.insertOne(doc);

        } else {
            System.out.println("--------");
            System.out.println(Node.class.getSimpleName());
            System.out.println(entity.getType().name());
        }
    }

    public void complete() {

    }

    public void initialize(Map<String, Object> metaData) {
        logger.fine("initialize() with metadata: " + metaData.toString());
    }

    public void close() {

    }
}
