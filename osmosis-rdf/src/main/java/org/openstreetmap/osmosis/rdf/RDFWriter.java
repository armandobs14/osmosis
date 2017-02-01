package org.openstreetmap.osmosis.rdf;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
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

    private Logger logger = Logger.getLogger(RDFWriter.class.getName());
    private Document tags = null;

    private File file = null;
    private final FileWriter fileWriter;

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
    }

    @Override
    public void initialize(Map<String, Object> metaData) {
        logger.fine("initialize() with metadata: " + metaData.toString());
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

        switch (entity.getType()) {
            case Node:
                Node node = (Node) entity;
                TagCollection tagCollection = (TagCollection) node.getTags();
                String key = null;
                String value = null;
                Map<String, String> buildMap = tagCollection.buildMap();
                for (Map.Entry<String, String> entry : buildMap.entrySet()) {
                    String k = entry.getKey();
                    if (tags.containsKey(k)) {
                        String v = entry.getValue();
                        List<String> list = tags.get(k, List.class);
                        if (list.contains(v)) {
                            key = k;
                            value = v;
                        }
                    }
                }
                if (key != null) {
                    buildMap.remove(key);
                    try {
                        String[] value_split = value.split("[_-]");
                        StringBuilder value_URI = new StringBuilder();
                        value_URI.append(OSM);
                        for (int i = 0; i < value_split.length; i++) {
                            value_URI.append(WordUtils.capitalizeFully(value_split[i]));
                        }

                        String resource = "<" + value_URI.toString() + "/" + entity.getId() + ">";

                        fileWriter.append(resource)
                                .append(" rdf:type ")
                                .append(" <" + value_URI.toString() + "> ")
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
                                default:
                            }
                        }
                        fileWriter.append(" osm:hasOSMID ")
                                .append(" \"" + String.valueOf(entity.getId()) + "\"^^xsd:int ")
                                .append(";\n");

                        // address resource
                        String adr_URI = "<" + OSM + "address/" + entity.getId() + ">";
                        fileWriter.append(" loc:hasAddress ")
                                .append(adr_URI)
                                .append(".\n");

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
                    } catch (IOException ex) {

                    }
                }
                break;
            default:
                break;
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
