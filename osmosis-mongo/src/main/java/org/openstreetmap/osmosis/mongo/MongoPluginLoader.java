package org.openstreetmap.osmosis.mongo;

/**
 * Created by armando on 10/10/16.
 */
import org.openstreetmap.osmosis.core.pipeline.common.TaskManagerFactory;
import org.openstreetmap.osmosis.core.plugin.PluginLoader;
import java.util.HashMap;
import java.util.Map;

public class MongoPluginLoader implements PluginLoader {

    public Map<String, TaskManagerFactory> loadTaskFactories() {
        Map<String, TaskManagerFactory> map = new HashMap();
        map.put("write-mongo", new MongoWriterFactory());
        map.put("write-mongo-simple", new MongoWriterSimpleFactory());
        return map;
    }
}
