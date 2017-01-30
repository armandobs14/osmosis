package org.openstreetmap.osmosis.rdf;

/**
 * Created by williams on 27/01/17.
 */
import org.openstreetmap.osmosis.core.pipeline.common.TaskManagerFactory;
import org.openstreetmap.osmosis.core.plugin.PluginLoader;
import java.util.HashMap;
import java.util.Map;

public class RDFPluginLoader implements PluginLoader {

    @Override
    public Map<String, TaskManagerFactory> loadTaskFactories() {
        Map<String, TaskManagerFactory> map = new HashMap();
        RDFWriterFactory rdfWriterFactory = new RDFWriterFactory();
        map.put("write-rdf", rdfWriterFactory);
        return map;
    }
}
