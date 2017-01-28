package org.openstreetmap.osmosis.rdf;

import org.openstreetmap.osmosis.core.pipeline.common.RunnableTaskManager;
import org.openstreetmap.osmosis.core.pipeline.common.TaskConfiguration;
import org.openstreetmap.osmosis.core.pipeline.common.TaskManager;
import org.openstreetmap.osmosis.core.pipeline.common.TaskManagerFactory;
import org.openstreetmap.osmosis.core.pipeline.v0_6.SinkManager;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by williams on 27/01/17.
 */
public class RDFWriterFactory extends TaskManagerFactory {
    protected TaskManager createTaskManagerImpl(TaskConfiguration taskConfig) {
        Sink sink = null;
        System.out.println("---------");
        System.out.println(taskConfig.getConfigArgs());
        System.out.println(taskConfig.getPipeArgs());
        System.out.println(taskConfig.getDefaultArg());

        Map<String, String> configArgs = taskConfig.getConfigArgs();

        try{
            sink= new RDFWriter();
        }catch (Exception e){
            System.out.println(taskConfig.getDefaultArg());
            System.out.println(taskConfig.getPipeArgs());
            System.out.println(taskConfig.getConfigArgs());
        }
        return new SinkManager(taskConfig.getId(),sink,taskConfig.getPipeArgs());
    }

}
