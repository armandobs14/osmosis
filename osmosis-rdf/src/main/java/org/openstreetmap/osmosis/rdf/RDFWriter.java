package org.openstreetmap.osmosis.rdf;

import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.*;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;
import org.openstreetmap.osmosis.core.util.LazyHashMap;

import java.util.*;
import java.util.logging.Logger;

/**
 * Created by Williams on 27/01/17.
 */
public class RDFWriter implements Sink {

    private final String ONTO_OSM = "http://linkn.com.br/onto/osm/";

    Logger logger = Logger.getLogger(RDFWriter.class.getName());
    private String host;

    public RDFWriter() {

    }


    public void process(EntityContainer entityContainer) {

        Entity entity = entityContainer.getEntity();
        EntityType type = entity.getType();

        if(entity.getType().equals(EntityType.Node)){
            Node node = (Node) entity;

            TagCollection tagCollection = (TagCollection) node.getTags();
            Map<String, Object> map = new HashMap<>();

            tagCollection.buildMap().forEach((k,v) ->{
                //System.out.println(k + " - " + v); //created_by - JOSM
                //System.out.println(entity.toString() + "\n\n"); //Node(id=94754991, #tags=1)
                map.put(k.replace(".","_"),v);
            });


            //Point point = new Point(new Position(node.getLongitude(),node.getLatitude()));
            //map.put("loc",point);

            //metadata
            map.put("_id",entity.getId());
            map.put("version",entity.getVersion());
            map.put("timestamps",entity.getTimestamp().getTime());

//            System.out.println(map.toString());
            if(map.containsKey("name")){
                System.out.println(map.get("name"));
            }
            if(map.containsKey("value")){
                System.out.println(map.get("value"));
                System.out.println("\n");
            }


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
