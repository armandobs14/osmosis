package org.openstreetmap.osmosis.rdf;

import org.openstreetmap.osmosis.core.pipeline.common.TaskConfiguration;
import org.openstreetmap.osmosis.core.pipeline.common.TaskManager;
import org.openstreetmap.osmosis.core.pipeline.common.TaskManagerFactory;
import org.openstreetmap.osmosis.core.pipeline.v0_6.SinkManager;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;

/**
 * Created by williams on 27/01/17.
 */
public class RDFWriterFactory extends TaskManagerFactory {

    @Override
    protected TaskManager createTaskManagerImpl(TaskConfiguration taskConfig) {

        SinkManager sinkManager = null;
        try {
            Sink sink = new RDFWriter();
            sinkManager = new SinkManager(taskConfig.getId(), sink, taskConfig.getPipeArgs());
        } catch (Exception e) {
            System.out.println(taskConfig.getDefaultArg());
            System.out.println(taskConfig.getPipeArgs());
            System.out.println(taskConfig.getConfigArgs());
        }
        return sinkManager;
    }

}
