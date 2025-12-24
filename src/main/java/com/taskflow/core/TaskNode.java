package com.taskflow.core;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A concrete implementation of a Task that can be part of a DAG.
 * Thread-safe implementation ensuring proper atomicity for task state.
 */
public abstract class TaskNode implements Task {
    private final String id;
    private final Set<TaskNode> dependencies = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean completed = new AtomicBoolean(false);
    private final AtomicBoolean executing = new AtomicBoolean(false);
    
    public TaskNode(String id) {
        this.id = id;
    }
    
    @Override
    public String getId() {
        return id;
    }
    
    @Override
    public boolean isReady() {
        // A task is ready if all its dependencies are completed and it's not already executing
        if (isCompleted() || isExecuting()) {
            return false;
        }
        return dependencies.stream().allMatch(TaskNode::isCompleted);
    }
    
    @Override
    public void markCompleted() {
        completed.set(true);
        executing.set(false);
    }
    
    @Override
    public boolean isCompleted() {
        return completed.get();
    }
    
    /**
     * Marks this task as currently executing.
     * This prevents the task from being scheduled multiple times.
     * 
     * @return true if the task was successfully marked as executing, false if already executing
     */
    public boolean markExecuting() {
        return executing.compareAndSet(false, true);
    }
    
    /**
     * Checks if this task is currently executing.
     * 
     * @return true if the task is executing
     */
    public boolean isExecuting() {
        return executing.get();
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
     * @return unmodifiable set of dependencies
     */
    public Set<TaskNode> getDependencies() {
        return Collections.unmodifiableSet(dependencies);
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TaskNode taskNode = (TaskNode) o;
        return id.equals(taskNode.id);
    }
    
    @Override
    public int hashCode() {
        return id.hashCode();
    }
}
