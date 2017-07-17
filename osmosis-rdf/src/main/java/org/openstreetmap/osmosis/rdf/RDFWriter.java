package org.openstreetmap.osmosis.rdf;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.*;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;
import java.util.Map;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.bson.Document;
import org.apache.commons.lang3.text.WordUtils;
import org.bson.conversions.Bson;

/**
 * Created by Williams on 27/01/17.
 */
public class RDFWriter implements Sink {

    private final String LOC = "http://linkn.com.br/onto/locality/";
    private final String OSM = "http://linkn.com.br/onto/osm/";
    private final String RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
    private final String XSD = "http://www.w3.org/2001/XMLSchema#";

    private Document tags = null;

    private Map<String, FileWriter> files = null;

    private final MongoCollection<Document> collection;

    private final String EXT_TURTLE = ".ttl";
    private final String EXT_GRAPH = ".graph";

    private final String GRAPH_BASE = "http://linkn.com.br/data/building/";
    public final String host = "/home/williams/projetos/dados/br";

    private File directory;

    final long total_points;
    int actual = 0;

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

        files = new HashMap<>();

        collection = new MongoClient("172.17.0.1").getDatabase("main").getCollection("node");
        total_points = collection.count();
    }

    @Override
    public void initialize(Map<String, Object> metaData) {
        directory = new File(host);
        if (!directory.exists()) {
            directory.mkdirs();
        } else {
            File[] f = directory.listFiles();
            for (int i = 0; i < f.length; i++) {
                File ff = f[0];
                ff.delete();
            }
        }
    }

    private void header(Writer writer) throws IOException {
        writer.append("@prefix rdf: <").append(RDF).append(">.\n");
        writer.append("@prefix osm: <").append(OSM).append(">.\n");
        writer.append("@prefix loc: <").append(LOC).append(">.\n");
        writer.append("@prefix xsd: <").append(XSD).append(">.\n");
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
            System.out.println(++actual + " de " + total_points);

            Document n = getNode(entity.getId());
            try {

                String dataset = null;
                if (n != null && n.containsKey("dataset")) {
                    dataset = n.getString("dataset");
                }

                FileWriter fw = solveFile(dataset);

                // address resource
                String adr_URI = "<" + OSM + "address/" + entity.getId() + ">";
                switch (entity.getType()) {
                    case Node: {
                        Node node = (Node) entity;

                        fw.append(adr_URI)
                                .append(" rdf:type loc:Address;");
                        fw.append("loc:hasLatitude ")
                                .append("\"" + node.getLatitude() + "\"^^xsd:string;");
                        fw.append("loc:hasLongitude ")
                                .append("\"" + node.getLongitude() + "\"^^xsd:string");

                        if (n.containsKey("city")) {
                            fw.append("; loc:isComposedOf <").append(n.getString("city")).append(">");
                        }
                        fw.append(".\n");
                        break;
                    }
                    case Way: {
                        //Way way = (Way) entity;
                    }
                    case Relation: {
                        //Relation rel = (Relation) entity;                        
                        return;
                    }
                }

                String resource_URI = "<" + OSM + entity.getType().name().toLowerCase() + "/" + entity.getId() + ">";
                fw.append(resource_URI).append(" osm:publisher osm:OpenStreetMap; ");
                for (Map.Entry<String, String> entry : keys_values.entrySet()) {
                    buildMap.remove(entry.getKey());

                    String[] value_split = entry.getValue().split("[_-]");

                    StringBuilder type = new StringBuilder();
                    for (String value_split1 : value_split) {
                        type.append(WordUtils.capitalizeFully(value_split1));
                    }

                    fw.append("rdf:type ")
                            .append("osm:" + type.toString())
                            .append(";");
                }
                fw.append("osm:hasOSMID ")
                        .append("\"" + entity.getId() + "\"^^xsd:integer;");
                for (Map.Entry<String, String> entry : buildMap.entrySet()) {
                    String k = entry.getKey();
                    String v = entry.getValue();
                    switch (k) {
                        case "name":
                            fw.append("osm:hasName ")
                                    .append("\"" + v.replaceAll("((\\\")|(\\\\))", "") + "\"^^xsd:string;");
                            break;
                        case "publisher":
                            fw.append("osm:publisher ")
                                    .append("osm:" + v.replaceAll(" ", ""))
                                    .append(";");
                            break;
                    }
                }

                fw.append("loc:hasAddress ")
                        .append(adr_URI)
                        .append(".\n");
                //writing data from stream to file
                fw.flush();
            } catch (IOException ex) {
                ex.getLocalizedMessage();
            }
        }
    }

    @Override
    public void complete() {
        for (Map.Entry<String, FileWriter> entry : files.entrySet()) {
            try {
                entry.getValue().close();
            } catch (IOException ex) {
                System.out.println(entry.getKey());
                Logger.getLogger(RDFWriter.class.getName()).log(Level.SEVERE, null, ex);
            }

        }
    }

    @Override
    public void close() {
    }

    private FileWriter solveFile(String dataset) throws IOException {
        StringBuilder file_name = new StringBuilder();
        if (dataset == null) {
            file_name.append(directory.getName());
            dataset = directory.getName();
        } else {
            String[] dir_file = dataset.toLowerCase().split("/");
            file_name.append(dir_file[dir_file.length - 1]);
        }

        if (files.containsKey(dataset)) {
            return files.get(dataset);
        } else {
            file_name.append(EXT_TURTLE);

            File file = new File(directory, file_name.toString());
            FileWriter fw;
            if (!file.exists()) {
                file.createNewFile();
                fw = new FileWriter(file, true);
                header(fw);
                fw.flush();

                files.put(dataset, fw);
            } else {
                fw = new FileWriter(file, true);
            }
            File file_graph = new File(file.getAbsolutePath() + EXT_GRAPH);
            if (!file_graph.exists()) {
                file_graph.createNewFile();
                FileWriter fw_graph = new FileWriter(file_graph);
                fw_graph.append(GRAPH_BASE + dataset.toLowerCase() + "/");
                fw_graph.close();
            }
            return fw;
        }
    }

    private final Bson projection = Projections.include("dataset", "city");

    public Document getNode(long id) {
        return collection.find(Filters.eq("_id", id)).projection(projection).first();
    }
}
