package org.openstreetmap.osmosis.rdf;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.*;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;
import java.util.Map;
import java.util.HashMap;
import java.util.logging.Logger;
import java.util.List;
import org.bson.Document;
import org.apache.commons.lang3.text.WordUtils;

/**
 * Created by Williams on 27/01/17.
 */
public class RDFWriter implements Sink {

    private final String OSM = "http://linkn.com.br/onto/osm/";
    private final String RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";

    private Logger logger = Logger.getLogger(RDFWriter.class.getName());
    private Document tags = null;

    public RDFWriter() throws FileNotFoundException, IOException {
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
        System.out.println("oioioio");
    }

    @Override
    public void process(EntityContainer entityContainer) {

        Entity entity = entityContainer.getEntity();

        if (entity.getType().equals(EntityType.Node)) {
            Node node = (Node) entity;
            TagCollection tagCollection = (TagCollection) node.getTags();
            Map<String, Object> map = new HashMap<>();
            boolean status = false;
            String key = null;
            String value = null;
            Map<String, String> buildMap = tagCollection.buildMap();
            for (Map.Entry<String, String> entry : buildMap.entrySet()) {
                String k = entry.getKey();
                String v = entry.getValue();
                List<String> list = tags.get(k, List.class);
                if (list.contains(v)) {
                    status = true;
                    key = k;
                    value = v;
                }
            }
            if (key != null) {
                buildMap.remove(key);

                String[] value_split = value.split("[_-]");
                StringBuilder value_URI = new StringBuilder();
                value_URI.append(OSM);
                for (int i = 0; i < value_split.length; i++) {
                    value_URI.append(WordUtils.capitalizeFully(value_split[i]));
                }

                URI resource = URI.create(value_URI.toString() + "/" + entity.getId());
                map.put(OSM + "hasOSMID", entity.getId());
                map.put(RDF + "type", URI.create(value_URI.toString()));

                for (Map.Entry<String, String> entry : buildMap.entrySet()) {
                    String k = entry.getKey();
                    String v = entry.getValue();
                    switch (k) {
                        case "name":
                            map.put(OSM + "hasName", v);
                            break;
                        default:
                            map.put(k.replace(".", "_"), v);
                    }
                }

            }

            //Point point = new Point(new Position(node.getLongitude(),node.getLatitude()));
            //map.put("loc",point);            
        } else {
            System.out.println("--------");
            System.out.println(Node.class.getSimpleName());
            System.out.println(entity.getType().name());
        }
    }

    @Override
    public void complete() {

    }

    @Override
    public void initialize(Map<String, Object> metaData
    ) {
        System.out.println("metadata: " + metaData.toString());
        logger.fine("initialize() with metadata: " + metaData.toString());
    }

    @Override
    public void close() {

    }
}
