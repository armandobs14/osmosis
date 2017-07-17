package org.openstreetmap.osmosis.mongo;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.geojson.*;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.bson.Document;
import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.*;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;
import java.util.logging.Logger;

/**
 * Created by armando on 10/10/16.
 */
public class MongoWriterSimple implements Sink {

    Logger logger = Logger.getLogger(MongoWriterSimple.class.getName());

    private final Document tags;
    private final MongoCollection<Document> nodeCollection;

    public MongoWriterSimple(MongoClient client) throws FileNotFoundException, IOException {
        MongoDatabase db = client.getDatabase("main");
        db.getCollection("node").createIndex(new Document("loc", "2dsphere"));
        nodeCollection = db.getCollection("node");

        FileReader reader = new FileReader("/home/williams/projetos/osmosis/osmosis-rdf/conf/tags.json");
        BufferedReader buffer = new BufferedReader(reader);
        StringBuilder text_tags = new StringBuilder();
        String line;
        while ((line = buffer.readLine()) != null) {
            text_tags.append(line);
        }
        buffer.close();
        reader.close();

        tags = Document.parse(text_tags.toString());

    }

    @Override
    public void process(EntityContainer entityContainer) {

        Entity entity = entityContainer.getEntity();

        TagCollection tagCollection = (TagCollection) entity.getTags();

        Map<String, String> buildMap = tagCollection.buildMap();

        Map<String, String> keys_values = new HashMap<>();
        for (Map.Entry<String, String> entry : buildMap.entrySet()) {
            String k = entry.getKey();
            if (tags.containsKey(k)) {
                String v = entry.getValue();
                switch (v) {
                    case "street":
                    case "neighborhood":
                    case "suburb":
                    case "village":
                    case "hamlet":
                    case "town":
                    case "city":
                    case "state":
                    case "country":
                        break;
                    default:
                        List<String> list = tags.get(k, List.class);
                        if (list.contains(v)) {
                            keys_values.put(k, v);
                        }
                }
            }
        }
        if (!keys_values.isEmpty()) {
            if (entity.getType().equals(EntityType.Node)) {
                Node node = (Node) entity;

                Document doc = new Document();

                Point point = new Point(new Position(node.getLongitude(), node.getLatitude()));
                doc.put("loc", point);
                doc.put("_id", entity.getId());

                nodeCollection.insertOne(doc);

            } else {
                System.out.println("--------");
                System.out.println(Node.class.getSimpleName());
                System.out.println(entity.getType().name());
            }
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
