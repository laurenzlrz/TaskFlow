package com.taskflow.logging;

/**
 * Contains all logging messages used throughout the TaskFlow framework.
 * This centralizes all logging strings to avoid magic strings in code.
 */
public final class LogMessages {
    
    // WorkerPool messages
    public static final String WORKER_POOL_CREATED = "WorkerPool created with %d core threads, %d max threads, %d %s keepAliveTime";
    public static final String WORKER_POOL_SHUTDOWN_REJECTED = "WorkerPool has been shut down";
    public static final String AUTO_SHUTDOWN_ENABLED = "Auto-shutdown on idle enabled: %d %s";
    public static final String AUTO_SHUTDOWN_DISABLED = "Auto-shutdown on idle disabled";
    public static final String WORKER_POOL_IDLE = "WorkerPool idle for %d ms, initiating shutdown";
    public static final String WORKER_POOL_SHUTTING_DOWN = "Shutting down WorkerPool";
    public static final String WORKER_POOL_FORCE_SHUTDOWN = "Forcing immediate shutdown of WorkerPool";
    public static final String WORKER_POOL_SHUTDOWN_TIMEOUT = "WorkerPool did not terminate in time, forcing shutdown";
    public static final String WORKER_POOL_SHUTDOWN_SUCCESS = "WorkerPool shutdown completed successfully";
    public static final String WORKER_POOL_SHUTDOWN_INTERRUPTED = "Shutdown interrupted, forcing immediate shutdown";
    
    // ParallelRunBundle messages
    public static final String BUNDLE_CANNOT_ADD_TASKS = "Cannot add tasks while bundle is running";
    public static final String BUNDLE_ALREADY_RUNNING = "Bundle is already running";
    public static final String BUNDLE_STARTING = "Starting execution of %d tasks";
    public static final String BUNDLE_COMPLETED = "Execution completed: %d succeeded, %d failed, %d ms";
    public static final String BUNDLE_SUBMITTING_TASK = "Submitting task: %s";
    public static final String BUNDLE_EXECUTING_TASK = "Executing task: %s";
    public static final String BUNDLE_TASK_COMPLETED = "Task completed: %s";
    public static final String BUNDLE_TASK_FAILED = "Task failed: %s - %s";
    public static final String BUNDLE_TASK_EXECUTION_ERROR = "Task execution error: %s - %s";
    public static final String BUNDLE_TASK_EXECUTION_FAILED = "Task execution failed: %s - %s";
    public static final String BUNDLE_INTERRUPTED = "Interrupted while checking task completion";
    public static final String BUNDLE_TASK_EXECUTION_FAILED_WRAPPER = "Task execution failed: %s";
    public static final String BUNDLE_EXECUTION_FAILED_MESSAGE = "Task execution failed";
    
    // PipelineOrchestrator messages
    public static final String ORCHESTRATOR_CREATED_WITH_AUTO_SHUTDOWN = "PipelineOrchestrator created with auto-shutdown after %d %s of inactivity";
    public static final String ORCHESTRATOR_CREATED_WITHOUT_AUTO_SHUTDOWN = "PipelineOrchestrator created without auto-shutdown";
    public static final String ORCHESTRATOR_WORKER_POOL_SHUTDOWN = "WorkerPool has been shut down";
    public static final String ORCHESTRATOR_EXECUTING_BUNDLE = "Executing bundle with %d tasks";
    public static final String ORCHESTRATOR_BUNDLES_COMPLETED = "Completed execution of %d bundles";
    public static final String ORCHESTRATOR_SHUTTING_DOWN = "Shutting down PipelineOrchestrator";
    public static final String ORCHESTRATOR_SHUTTING_DOWN_WITH_TIMEOUT = "Shutting down PipelineOrchestrator with %d %s timeout";
    public static final String ORCHESTRATOR_FORCE_SHUTDOWN = "Force shutting down PipelineOrchestrator";
    
    private LogMessages() {
        // Prevent instantiation
    }
}
