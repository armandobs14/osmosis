package org.openstreetmap.osmosis.rdf;

import com.joint.KAO;
import com.joint.MongoManager;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.*;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;
import java.util.Map;
import java.util.logging.Logger;
import java.util.List;
import java.util.logging.Level;
import org.bson.Document;
import org.apache.commons.lang3.text.WordUtils;

/**
 * Created by Williams on 27/01/17.
 */
public class RDFWriter implements Sink {

    private final String LOC = "http://linkn.com.br/onto/locality/";
    private final String OSM = "http://linkn.com.br/onto/osm/";
    private final String RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";

    private Document tags = null;

    private File file = null;
    private final FileWriter fileWriter;
    private MongoManager mongo_mng;

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

        file = new File("/home/williams/projetos/dados/osm.ttl");
        fileWriter = new FileWriter(file, true);
        mongo_mng = new MongoManager();
    }

    @Override
    public void initialize(Map<String, Object> metaData) {
        try {
            fileWriter.append("@prefix rdf: <").append(RDF).append(">.\n");
            fileWriter.append("@prefix osm: <").append(OSM).append(">.\n");
            fileWriter.append("@prefix loc: <").append(LOC).append(">.\n");
        } catch (IOException ex) {
            Logger.getLogger(RDFWriter.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public void process(EntityContainer entityContainer) {

        Entity entity = entityContainer.getEntity();

        TagCollection tagCollection = (TagCollection) entity.getTags();
        Map<String, String> keys_values = new HashMap<>();

        Map<String, String> buildMap = tagCollection.buildMap();
        for (Map.Entry<String, String> entry : buildMap.entrySet()) {
            String k = entry.getKey();
            if (tags.containsKey(k)) {
                String v = entry.getValue();
                switch (v) {
                    case "street":
                    case "suburb":
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
            try {
                String resource_URI = "<" + OSM + entity.getType().name().toLowerCase() + "/" + entity.getId() + ">";
                fileWriter.append(resource_URI).append(" osm:publisher osm:OpenStreetMap; ");
                for (Map.Entry<String, String> entry : keys_values.entrySet()) {
                    buildMap.remove(entry.getKey());

                    String[] value_split = entry.getValue().split("[_-]");

                    StringBuilder type = new StringBuilder();
                    for (String value_split1 : value_split) {
                        type.append(WordUtils.capitalizeFully(value_split1));
                    }

                    fileWriter.append(" rdf:type ")
                            .append(" osm:" + type.toString())
                            .append(";\n");
                }
                fileWriter.append(" osm:hasOSMID ")
                        .append(" \"" + entity.getId() + "\"^^xsd:int ")
                        .append(";\n");
                for (Map.Entry<String, String> entry : buildMap.entrySet()) {
                    String k = entry.getKey();
                    String v = entry.getValue();
                    switch (k) {
                        case "name":
                            fileWriter.append(" osm:hasName ")
                                    .append(" \"" + v + "\"^^xsd:string ")
                                    .append(";\n");
                            break;
                        case "publisher":
                            fileWriter.append(" osm:publisher ")
                                    .append(" osm:" + v.replaceAll(" ", ""))
                                    .append(";\n");
                            break;
                    }
                }
                // address resource
                String adr_URI = "<" + OSM + "address/" + entity.getId() + ">";
                fileWriter.append(" loc:hasAddress ")
                        .append(adr_URI)
                        .append(".\n");
                switch (entity.getType()) {
                    case Node: {
                        Node node = (Node) entity;

                        fileWriter.append(adr_URI)
                                .append(" rdf:type ")
                                .append(" loc:Address ")
                                .append(";\n");
                        fileWriter.append(" loc:hasLatitude ")
                                .append(" \"" + node.getLatitude() + "\"^^xsd:string ")
                                .append(";\n");
                        fileWriter.append(" loc:hasLongitude ")
                                .append(" \"" + node.getLongitude() + "\"^^xsd:string ")
                                .append(".\n");
                        Document city_dataset = mongo_mng.contains(node.getLongitude(), node.getLatitude());
                        System.out.println(city_dataset);
                        if (city_dataset != null) {
                            
                            fileWriter.append(adr_URI)
                                    .append(" loc:isComposedOf ")
                                    .append("<" + city_dataset.getString("@id") + "> ")
                                    .append(".\n");
                            fileWriter.append(resource_URI)
                                    .append(" <dataset> ")
                                    .append(city_dataset.getString("dataset"))
                                    .append(".\n");
                        }
                        break;
                    }
                    case Way: {
                        //Way way = (Way) entity;
                        break;
                    }
                    case Relation: {
                        //Relation rel = (Relation) entity;
                    }
                }
            } catch (IOException ex) {
                System.out.println(ex.getLocalizedMessage());
            }
        }
    }

    @Override
    public void complete() {
    }

    @Override
    public void close() {
        try {
            fileWriter.close();
        } catch (IOException ex) {
            Logger.getLogger(RDFWriter.class
                    .getName()).log(Level.SEVERE, null, ex);
        }
    }
}
