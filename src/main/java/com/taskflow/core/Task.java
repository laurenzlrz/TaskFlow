package com.taskflow.core;

/**
 * Represents a task that can be executed in the pipeline.
 * Tasks are nodes in the DAG (Directed Acyclic Graph).
 */
public interface Task {
    /**
     * Executes the task.
     * @throws Exception if execution fails
     */
    void execute() throws Exception;
    
    /**
     * Gets the unique identifier for this task.
     * @return task ID
     */
    String getId();
    
    /**
     * Checks if this task is ready to be executed.
     * A task is ready when all its dependencies have completed.
     * @return true if the task can be executed
     */
    boolean isReady();
    
    /**
     * Marks the task as completed.
     */
    void markCompleted();
    
    /**
     * Checks if the task has been completed.
     * @return true if completed
     */
    boolean isCompleted();
}
