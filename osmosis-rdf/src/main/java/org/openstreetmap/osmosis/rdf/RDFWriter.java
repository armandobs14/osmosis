package org.openstreetmap.osmosis.rdf;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.*;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;
import java.util.Map;
import java.util.logging.Logger;
import java.util.List;
import java.util.logging.Level;
import org.bson.Document;
import org.apache.commons.lang3.text.WordUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;

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
    private HttpClient client;

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

        file = new File("/home/williams/projetos/dados/osm.n4");
        fileWriter = new FileWriter(file, true);

        client = new DefaultHttpClient();
    }

    @Override
    public void process(EntityContainer entityContainer) {

        Entity entity = entityContainer.getEntity();

        if (entity.getType().equals(EntityType.Node)) {
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
                    String url = "http://api.boamoradia.com.br:8080/address?lat=" + node.getLatitude() + "&lon=" + node.getLongitude() + "&datatype=Building";

                    HttpGet request = new HttpGet(url);
                    HttpResponse response = client.execute(request);

                    BufferedReader rd = new BufferedReader(
                            new InputStreamReader(response.getEntity().getContent()));

                    StringBuilder result = new StringBuilder();
                    String line = "";
                    while ((line = rd.readLine()) != null) {
                        result.append(line);
                    }
                    Document adr = Document.parse(result.toString());
                    String dataset = "<http://linkn.com.br/data/building/br/" + adr.getString("state_uf").toLowerCase() + "/>";

                    String[] value_split = value.split("[_-]");
                    StringBuilder value_URI = new StringBuilder();
                    value_URI.append(OSM);
                    for (int i = 0; i < value_split.length; i++) {
                        value_URI.append(WordUtils.capitalizeFully(value_split[i]));
                    }

                    String resource = "<" + value_URI.toString() + "/" + entity.getId() + ">";

                    fileWriter.append(resource)
                            .append(" <" + RDF + "type> ")
                            .append(" <" + value_URI.toString() + "> ")
                            .append(dataset)
                            .append(".\n");
                    fileWriter.append(resource)
                            .append(" <" + OSM + "hasOSMID> ")
                            .append(" \"" + String.valueOf(entity.getId()) + "\"^^xsd:int ")
                            .append(dataset)
                            .append(".\n");
                    fileWriter.append(resource)
                            .append(" <" + LOC + "hasAddress> ")
                            .append(" <" + adr.getString("uri") + "> ")
                            .append(dataset)
                            .append(".\n");

                    for (Map.Entry<String, String> entry : buildMap.entrySet()) {
                        String k = entry.getKey();
                        String v = entry.getValue();
                        switch (k) {
                            case "name":
                                fileWriter.append(resource)
                                        .append(" <" + OSM + "hasName> ")
                                        .append(" \"" + v + "\"^^xsd:string ")
                                        .append(dataset)
                                        .append(".\n");
                                break;
                            default:
                        }
                    }
                } catch (IOException | org.bson.json.JsonParseException ex) {

                }
            }
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
    public void initialize(Map<String, Object> metaData) {
        logger.fine("initialize() with metadata: " + metaData.toString());
    }

    @Override
    public void close() {
        try {
            fileWriter.close();
        } catch (IOException ex) {
            Logger.getLogger(RDFWriter.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
