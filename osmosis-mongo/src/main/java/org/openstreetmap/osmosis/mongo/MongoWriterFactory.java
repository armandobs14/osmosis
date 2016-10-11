package org.openstreetmap.osmosis.mongo;

import org.openstreetmap.osmosis.core.pipeline.common.RunnableTaskManager;
import org.openstreetmap.osmosis.core.pipeline.common.TaskConfiguration;
import org.openstreetmap.osmosis.core.pipeline.common.TaskManager;
import org.openstreetmap.osmosis.core.pipeline.common.TaskManagerFactory;
import org.openstreetmap.osmosis.core.pipeline.v0_6.SinkManager;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by armando on 10/10/16.
 */
public class MongoWriterFactory extends TaskManagerFactory {
    protected TaskManager createTaskManagerImpl(TaskConfiguration taskConfig) {

        MongoWriter mongoWriter = new MongoWriter(taskConfig.getConfigArgs());
        return new SinkManager(taskConfig.getId(),mongoWriter,taskConfig.getPipeArgs());
    }

}
