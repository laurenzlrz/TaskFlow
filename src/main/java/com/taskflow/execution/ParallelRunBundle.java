package com.taskflow.execution;

import com.taskflow.core.Task;
import com.taskflow.core.TaskNode;
import com.taskflow.logging.LogMessages;
import com.taskflow.logging.TaskFlowLogger;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Executes a bundle of tasks in parallel using a WorkerPool.
 * Tasks are executed based on their dependencies (DAG structure).
 */
public class ParallelRunBundle {
    private final TaskFlowLogger logger;
    
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
     * @param logger the logger to use for this bundle
     */
    public ParallelRunBundle(WorkerPool workerPool, TaskFlowLogger logger) {
        this.workerPool = workerPool;
        this.logger = logger;
        this.tasks = ConcurrentHashMap.newKeySet();
        this.runningTasks = new ConcurrentHashMap<>();
        this.completedCount = new AtomicInteger(0);
        this.failedCount = new AtomicInteger(0);
        this.isRunning = false;
    }
    
    /**
     * Creates a ParallelRunBundle with the specified worker pool and default logger.
     * 
     * @param workerPool the worker pool to use for execution
     */
    public ParallelRunBundle(WorkerPool workerPool) {
        this(workerPool, TaskFlowLogger.forClass(ParallelRunBundle.class));
    }
    
    /**
     * Adds a task to this bundle.
     * 
     * @param task the task to add
     */
    public void addTask(TaskNode task) {
        if (isRunning) {
            throw new IllegalStateException(LogMessages.BUNDLE_CANNOT_ADD_TASKS);
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
            throw new IllegalStateException(LogMessages.BUNDLE_CANNOT_ADD_TASKS);
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
            throw new IllegalStateException(LogMessages.BUNDLE_ALREADY_RUNNING);
        }
        
        isRunning = true;
        completedCount.set(0);
        failedCount.set(0);
        runningTasks.clear();
        
        logger.info(LogMessages.BUNDLE_STARTING, tasks.size());
        long startTime = System.currentTimeMillis();
        
        try {
            proceedAll();
            
            long duration = System.currentTimeMillis() - startTime;
            logger.info(LogMessages.BUNDLE_COMPLETED, completedCount.get(), failedCount.get(), duration);
            
            return new ExecutionResult(completedCount.get(), failedCount.get(), duration);
        } finally {
            isRunning = false;
        }
    }
    
    /**
     * Proceeds with executing all ready tasks until all tasks are complete.
     */
    private void proceedAll() throws ExecutionException, InterruptedException {
        List<Exception> taskExceptions = new ArrayList<>();
        
        while (areNodesFree() || areNodesRunning()) {
            // Submit all ready tasks
            for (TaskNode task : tasks) {
                if (!task.isCompleted() && task.isReady() && !runningTasks.containsKey(task.getId())) {
                    // Use atomic markExecuting to prevent double execution
                    if (task.markExecuting()) {
                        submitTask(task);
                    }
                }
            }
            
            // Check for completed tasks and collect exceptions
            checkCompletedTasks(taskExceptions);
            
            // Wait using a more efficient approach than busy polling
            if (areNodesRunning()) {
                // Wait on the futures with a timeout instead of sleeping
                waitForNextCompletion(50);
            }
        }
        
        // Wait for all remaining tasks to complete
        waitForAllTasks(taskExceptions);
        
        // If there were any exceptions, throw the first one
        if (!taskExceptions.isEmpty()) {
            throw new ExecutionException(LogMessages.BUNDLE_EXECUTION_FAILED_MESSAGE, taskExceptions.get(0));
        }
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
        logger.fine(LogMessages.BUNDLE_SUBMITTING_TASK, task.getId());
        
        Future<?> future = workerPool.submit(() -> {
            try {
                logger.fine(LogMessages.BUNDLE_EXECUTING_TASK, task.getId());
                task.execute();
                task.markCompleted();
                completedCount.incrementAndGet();
                logger.fine(LogMessages.BUNDLE_TASK_COMPLETED, task.getId());
            } catch (Exception e) {
                failedCount.incrementAndGet();
                logger.severe(LogMessages.BUNDLE_TASK_FAILED, task.getId(), e.getMessage());
                throw new RuntimeException(String.format(LogMessages.BUNDLE_TASK_EXECUTION_FAILED_WRAPPER, task.getId()), e);
            }
        });
        
        runningTasks.put(task.getId(), future);
    }
    
    /**
     * Checks for completed tasks and removes them from the running tasks map.
     * Collects exceptions from failed tasks.
     */
    private void checkCompletedTasks(List<Exception> taskExceptions) {
        Iterator<Map.Entry<String, Future<?>>> iterator = runningTasks.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Future<?>> entry = iterator.next();
            if (entry.getValue().isDone()) {
                iterator.remove();
                try {
                    // Get result to propagate any exceptions
                    entry.getValue().get();
                } catch (ExecutionException e) {
                    logger.severe(LogMessages.BUNDLE_TASK_EXECUTION_ERROR, entry.getKey(), e.getCause().getMessage());
                    taskExceptions.add(e);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.warning(LogMessages.BUNDLE_INTERRUPTED);
                    taskExceptions.add(e);
                }
            }
        }
    }
    
    /**
     * Waits for the next task completion with a timeout.
     */
    private void waitForNextCompletion(long timeoutMs) {
        try {
            // Check if any future is done within the timeout
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (System.currentTimeMillis() < deadline && areNodesRunning()) {
                boolean anyDone = runningTasks.values().stream().anyMatch(Future::isDone);
                if (anyDone) {
                    break;
                }
                Thread.sleep(5);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
    
    /**
     * Waits for all running tasks to complete.
     * Collects exceptions from failed tasks.
     */
    private void waitForAllTasks(List<Exception> taskExceptions) throws InterruptedException {
        for (Map.Entry<String, Future<?>> entry : runningTasks.entrySet()) {
            try {
                entry.getValue().get();
            } catch (ExecutionException e) {
                logger.severe(LogMessages.BUNDLE_TASK_EXECUTION_FAILED, entry.getKey(), e.getCause().getMessage());
                taskExceptions.add(e);
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
