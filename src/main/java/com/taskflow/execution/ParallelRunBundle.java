package com.taskflow.execution;

import com.taskflow.core.Task;
import com.taskflow.core.TaskNode;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * Executes a bundle of tasks in parallel using a WorkerPool.
 * Tasks are executed based on their dependencies (DAG structure).
 */
public class ParallelRunBundle {
    private static final Logger LOGGER = Logger.getLogger(ParallelRunBundle.class.getName());
    
    private final WorkerPool workerPool;
    private final Set<TaskNode> tasks;
    private final Map<String, Future<?>> runningTasks;
    private final AtomicInteger completedCount;
    private final AtomicInteger failedCount;
    private volatile boolean isRunning;
    
    /**
     * Creates a ParallelRunBundle with the specified worker pool.
     * 
     * @param workerPool the worker pool to use for execution
     */
    public ParallelRunBundle(WorkerPool workerPool) {
        this.workerPool = workerPool;
        this.tasks = new HashSet<>();
        this.runningTasks = new ConcurrentHashMap<>();
        this.completedCount = new AtomicInteger(0);
        this.failedCount = new AtomicInteger(0);
        this.isRunning = false;
    }
    
    /**
     * Adds a task to this bundle.
     * 
     * @param task the task to add
     */
    public void addTask(TaskNode task) {
        if (isRunning) {
            throw new IllegalStateException("Cannot add tasks while bundle is running");
        }
        tasks.add(task);
    }
    
    /**
     * Adds multiple tasks to this bundle.
     * 
     * @param tasks the tasks to add
     */
    public void addTasks(Collection<TaskNode> tasks) {
        if (isRunning) {
            throw new IllegalStateException("Cannot add tasks while bundle is running");
        }
        this.tasks.addAll(tasks);
    }
    
    /**
     * Executes all tasks in the bundle, respecting their dependencies.
     * This method blocks until all tasks are complete or an error occurs.
     * 
     * @return execution result containing statistics
     * @throws ExecutionException if a task execution fails
     * @throws InterruptedException if execution is interrupted
     */
    public ExecutionResult execute() throws ExecutionException, InterruptedException {
        if (isRunning) {
            throw new IllegalStateException("Bundle is already running");
        }
        
        isRunning = true;
        completedCount.set(0);
        failedCount.set(0);
        runningTasks.clear();
        
        LOGGER.info(String.format("Starting execution of %d tasks", tasks.size()));
        long startTime = System.currentTimeMillis();
        
        try {
            proceedAll();
            
            long duration = System.currentTimeMillis() - startTime;
            LOGGER.info(String.format("Execution completed: %d succeeded, %d failed, %d ms",
                                     completedCount.get(), failedCount.get(), duration));
            
            return new ExecutionResult(completedCount.get(), failedCount.get(), duration);
        } finally {
            isRunning = false;
        }
    }
    
    /**
     * Proceeds with executing all ready tasks until all tasks are complete.
     */
    private void proceedAll() throws ExecutionException, InterruptedException {
        while (areNodesFree() || areNodesRunning()) {
            // Submit all ready tasks
            for (TaskNode task : tasks) {
                if (!task.isCompleted() && task.isReady() && !runningTasks.containsKey(task.getId())) {
                    submitTask(task);
                }
            }
            
            // Check for completed tasks
            checkCompletedTasks();
            
            // Small sleep to avoid busy waiting
            if (areNodesRunning()) {
                Thread.sleep(10);
            }
        }
        
        // Wait for all remaining tasks to complete
        waitForAllTasks();
    }
    
    /**
     * Checks if there are any tasks that are ready to run but not yet running.
     * 
     * @return true if there are free (ready) nodes
     */
    private boolean areNodesFree() {
        return tasks.stream()
            .anyMatch(task -> !task.isCompleted() && task.isReady() && !runningTasks.containsKey(task.getId()));
    }
    
    /**
     * Checks if there are any tasks currently running.
     * 
     * @return true if nodes are running
     */
    private boolean areNodesRunning() {
        return !runningTasks.isEmpty();
    }
    
    /**
     * Submits a task for execution.
     * 
     * @param task the task to submit
     */
    private void submitTask(TaskNode task) {
        LOGGER.fine(String.format("Submitting task: %s", task.getId()));
        
        Future<?> future = workerPool.submit(() -> {
            try {
                LOGGER.fine(String.format("Executing task: %s", task.getId()));
                task.execute();
                task.markCompleted();
                completedCount.incrementAndGet();
                LOGGER.fine(String.format("Task completed: %s", task.getId()));
            } catch (Exception e) {
                failedCount.incrementAndGet();
                LOGGER.severe(String.format("Task failed: %s - %s", task.getId(), e.getMessage()));
                throw new RuntimeException("Task execution failed: " + task.getId(), e);
            }
        });
        
        runningTasks.put(task.getId(), future);
    }
    
    /**
     * Checks for completed tasks and removes them from the running tasks map.
     */
    private void checkCompletedTasks() {
        Iterator<Map.Entry<String, Future<?>>> iterator = runningTasks.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Future<?>> entry = iterator.next();
            if (entry.getValue().isDone()) {
                iterator.remove();
                try {
                    // Get result to propagate any exceptions
                    entry.getValue().get();
                } catch (ExecutionException e) {
                    LOGGER.severe(String.format("Task execution error: %s", entry.getKey()));
                    // Exception is already counted in submitTask
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOGGER.warning("Interrupted while checking task completion");
                }
            }
        }
    }
    
    /**
     * Waits for all running tasks to complete.
     */
    private void waitForAllTasks() throws ExecutionException, InterruptedException {
        for (Map.Entry<String, Future<?>> entry : runningTasks.entrySet()) {
            try {
                entry.getValue().get();
            } catch (ExecutionException e) {
                LOGGER.severe(String.format("Task execution failed: %s", entry.getKey()));
                throw e;
            }
        }
        runningTasks.clear();
    }
    
    /**
     * Gets the number of tasks in this bundle.
     * 
     * @return task count
     */
    public int getTaskCount() {
        return tasks.size();
    }
    
    /**
     * Checks if the bundle is currently executing.
     * 
     * @return true if running
     */
    public boolean isRunning() {
        return isRunning;
    }
    
    /**
     * Result of executing a bundle of tasks.
     */
    public static class ExecutionResult {
        private final int successCount;
        private final int failureCount;
        private final long durationMs;
        
        public ExecutionResult(int successCount, int failureCount, long durationMs) {
            this.successCount = successCount;
            this.failureCount = failureCount;
            this.durationMs = durationMs;
        }
        
        public int getSuccessCount() {
            return successCount;
        }
        
        public int getFailureCount() {
            return failureCount;
        }
        
        public long getDurationMs() {
            return durationMs;
        }
        
        public boolean isSuccess() {
            return failureCount == 0;
        }
        
        @Override
        public String toString() {
            return String.format("ExecutionResult{success=%d, failed=%d, duration=%dms}",
                               successCount, failureCount, durationMs);
        }
    }
}
