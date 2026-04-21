package org.example.datapipeline.plugin;

public interface ActionPlugin {
    String getType();
    String getName();
    Executor getExecutor();
}
