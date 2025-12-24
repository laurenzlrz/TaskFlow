package com.taskflow.core;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A concrete implementation of a Task that can be part of a DAG.
 */
public abstract class TaskNode implements Task {
    private final String id;
    private final Set<TaskNode> dependencies = new HashSet<>();
    private final AtomicBoolean completed = new AtomicBoolean(false);
    
    public TaskNode(String id) {
        this.id = id;
    }
    
    @Override
    public String getId() {
        return id;
    }
    
    @Override
    public boolean isReady() {
        // A task is ready if all its dependencies are completed
        return dependencies.stream().allMatch(TaskNode::isCompleted);
    }
    
    @Override
    public void markCompleted() {
        completed.set(true);
    }
    
    @Override
    public boolean isCompleted() {
        return completed.get();
    }
    
    /**
     * Adds a dependency to this task.
     * @param dependency the task that must complete before this one
     */
    public void addDependency(TaskNode dependency) {
        dependencies.add(dependency);
    }
    
    /**
     * Gets all dependencies of this task.
     * @return set of dependencies
     */
    public Set<TaskNode> getDependencies() {
        return new HashSet<>(dependencies);
    }
}
