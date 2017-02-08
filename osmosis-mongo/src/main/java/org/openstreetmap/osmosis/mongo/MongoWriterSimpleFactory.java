package org.openstreetmap.osmosis.mongo;

import com.mongodb.MongoClient;
import org.openstreetmap.osmosis.core.pipeline.common.TaskConfiguration;
import org.openstreetmap.osmosis.core.pipeline.common.TaskManager;
import org.openstreetmap.osmosis.core.pipeline.common.TaskManagerFactory;
import org.openstreetmap.osmosis.core.pipeline.v0_6.SinkManager;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;

/**
 * Created by armando on 10/10/16.
 */
public class MongoWriterSimpleFactory extends TaskManagerFactory {

    @Override
    protected TaskManager createTaskManagerImpl(TaskConfiguration taskConfig) {
        Sink sink = null;
        try {
            MongoClient client = new MongoClient("localhost");
            sink = new MongoWriterSimple(client);
        } catch (Exception e) {
            System.out.println(taskConfig.getDefaultArg());
            System.out.println(taskConfig.getPipeArgs());
            System.out.println(taskConfig.getConfigArgs());
        }
        return new SinkManager(taskConfig.getId(), sink, taskConfig.getPipeArgs());
    }

}
