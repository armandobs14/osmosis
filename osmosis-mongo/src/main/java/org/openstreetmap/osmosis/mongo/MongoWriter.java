package org.openstreetmap.osmosis.mongo;

import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoDatabase;
import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.*;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;
import org.openstreetmap.osmosis.core.util.LazyHashMap;

import java.util.Collection;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Created by armando on 10/10/16.
 */
public class MongoWriter implements Sink {
    Logger logger = Logger.getLogger(MongoWriter.class.getName());
    private MongoClientURI client;


    public MongoWriter(Map<String,String> params){
        client = new MongoClientURI(params.get("host"));

        System.out.println(params);
        System.exit(0);
    }


    public void process(EntityContainer entityContainer) {
        Entity entity = entityContainer.getEntity();
        EntityType type = entity.getType();

        if(entity.getType().equals(EntityType.Node)){
            Node node = (Node) entity;

            TagCollection tagCollection = (TagCollection) node.getTags();
            Map<String, String> map = tagCollection.buildMap();
            map.put("id",String.valueOf(entity.getId()));
            map.put("type",String.valueOf(entity.getType()));
            map.put("lat",String.valueOf(node.getLatitude()));
            map.put("lon",String.valueOf(node.getLongitude()));

            //metadata
            map.put("changesetId",String.valueOf(entity.getChangesetId()));
            map.put("user.id",String.valueOf(entity.getUser().getId()));
            map.put("user.name",String.valueOf(entity.getUser().getName()));
            map.put("hashCode",String.valueOf(entity.hashCode()));
            map.put("version",String.valueOf(entity.getVersion()));
            map.put("timestamps",String.valueOf(entity.getTimestamp().getTime()));

            if(map.containsKey("name")){
                System.out.println(map);
            }


            // Reading tags
            /*
            for (Tag tag : tags) {
                System.out.println(tag.getKey()+" - "+tag.getValue());
                System.out.println(tag);
            }
            */
            


        }else{
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
