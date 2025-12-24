package com.taskflow.orchestration;

import com.taskflow.core.TaskNode;
import com.taskflow.execution.ParallelRunBundle;
import com.taskflow.execution.WorkerPool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Orchestrates the execution of multiple pipeline bundles.
 * Manages the lifecycle of the WorkerPool including automatic shutdown on inactivity.
 */
public class PipelineOrchestrator {
    private static final Logger LOGGER = Logger.getLogger(PipelineOrchestrator.class.getName());
    
    private final WorkerPool workerPool;
    private final List<ParallelRunBundle> bundles;
    private final boolean autoShutdownEnabled;
    private final long idleTimeout;
    private final TimeUnit idleTimeoutUnit;
    
    /**
     * Creates a PipelineOrchestrator with custom WorkerPool configuration.
     * 
     * @param corePoolSize core thread pool size
     * @param maxPoolSize maximum thread pool size
     * @param keepAliveTime thread keep-alive time
     * @param keepAliveUnit time unit for keep-alive
     * @param enableAutoShutdown whether to enable automatic shutdown on idle
     * @param idleTimeout timeout before automatic shutdown when idle
     * @param idleTimeoutUnit time unit for idle timeout
     */
    public PipelineOrchestrator(int corePoolSize, int maxPoolSize, 
                               long keepAliveTime, TimeUnit keepAliveUnit,
                               boolean enableAutoShutdown, 
                               long idleTimeout, TimeUnit idleTimeoutUnit) {
        this.workerPool = new WorkerPool(corePoolSize, maxPoolSize, keepAliveTime, keepAliveUnit);
        this.bundles = new ArrayList<>();
        this.autoShutdownEnabled = enableAutoShutdown;
        this.idleTimeout = idleTimeout;
        this.idleTimeoutUnit = idleTimeoutUnit;
        
        if (autoShutdownEnabled) {
            workerPool.enableAutoShutdownOnIdle(idleTimeout, idleTimeoutUnit);
            LOGGER.info(String.format("PipelineOrchestrator created with auto-shutdown after %d %s of inactivity",
                                     idleTimeout, idleTimeoutUnit));
        } else {
            LOGGER.info("PipelineOrchestrator created without auto-shutdown");
        }
    }
    
    /**
     * Creates a PipelineOrchestrator with default settings.
     * - Core pool size: number of available processors
     * - Max pool size: 2x number of available processors
     * - Keep-alive time: 60 seconds
     * - Auto-shutdown: enabled after 5 minutes of inactivity
     */
    public PipelineOrchestrator() {
        this(Runtime.getRuntime().availableProcessors(),
             Runtime.getRuntime().availableProcessors() * 2,
             60L, TimeUnit.SECONDS,
             true,
             5L, TimeUnit.MINUTES);
    }
    
    /**
     * Creates a PipelineOrchestrator with custom auto-shutdown settings.
     * 
     * @param enableAutoShutdown whether to enable automatic shutdown
     * @param idleTimeout timeout before shutdown when idle
     * @param idleTimeoutUnit time unit for idle timeout
     */
    public PipelineOrchestrator(boolean enableAutoShutdown, long idleTimeout, TimeUnit idleTimeoutUnit) {
        this(Runtime.getRuntime().availableProcessors(),
             Runtime.getRuntime().availableProcessors() * 2,
             60L, TimeUnit.SECONDS,
             enableAutoShutdown,
             idleTimeout, idleTimeoutUnit);
    }
    
    /**
     * Creates a new bundle for executing tasks.
     * 
     * @return a new ParallelRunBundle
     */
    public ParallelRunBundle createBundle() {
        ParallelRunBundle bundle = new ParallelRunBundle(workerPool);
        bundles.add(bundle);
        return bundle;
    }
    
    /**
     * Executes a specific bundle.
     * 
     * @param bundle the bundle to execute
     * @return execution result
     * @throws ExecutionException if execution fails
     * @throws InterruptedException if execution is interrupted
     */
    public ParallelRunBundle.ExecutionResult executeBundle(ParallelRunBundle bundle) 
            throws ExecutionException, InterruptedException {
        if (workerPool.isShutdown()) {
            throw new IllegalStateException("WorkerPool has been shut down");
        }
        
        LOGGER.info(String.format("Executing bundle with %d tasks", bundle.getTaskCount()));
        return bundle.execute();
    }
    
    /**
     * Executes all bundles that have been created.
     * 
     * @return list of execution results for each bundle
     * @throws ExecutionException if execution fails
     * @throws InterruptedException if execution is interrupted
     */
    public List<ParallelRunBundle.ExecutionResult> executeAllBundles() 
            throws ExecutionException, InterruptedException {
        List<ParallelRunBundle.ExecutionResult> results = new ArrayList<>();
        
        for (ParallelRunBundle bundle : bundles) {
            results.add(executeBundle(bundle));
        }
        
        LOGGER.info(String.format("Completed execution of %d bundles", bundles.size()));
        return results;
    }
    
    /**
     * Shuts down the orchestrator after completing all tasks.
     * This is a graceful shutdown that waits for running tasks to complete.
     * 
     * @return true if shutdown completed successfully
     */
    public boolean shutdown() {
        LOGGER.info("Shutting down PipelineOrchestrator");
        return workerPool.shutdownGracefully();
    }
    
    /**
     * Shuts down the orchestrator with a custom timeout.
     * 
     * @param timeout maximum time to wait
     * @param unit time unit
     * @return true if shutdown completed within timeout
     */
    public boolean shutdown(long timeout, TimeUnit unit) {
        LOGGER.info(String.format("Shutting down PipelineOrchestrator with %d %s timeout", timeout, unit));
        return workerPool.shutdownGracefully(timeout, unit);
    }
    
    /**
     * Forces immediate shutdown, attempting to stop all running tasks.
     * 
     * @return list of tasks that never started
     */
    public List<Runnable> shutdownNow() {
        LOGGER.warning("Force shutting down PipelineOrchestrator");
        return workerPool.shutdownNow();
    }
    
    /**
     * Gets the underlying worker pool.
     * 
     * @return the WorkerPool instance
     */
    public WorkerPool getWorkerPool() {
        return workerPool;
    }
    
    /**
     * Checks if the orchestrator has been shut down.
     * 
     * @return true if shut down
     */
    public boolean isShutdown() {
        return workerPool.isShutdown();
    }
    
    /**
     * Gets the number of currently active worker threads.
     * 
     * @return active thread count
     */
    public int getActiveWorkerCount() {
        return workerPool.getActiveCount();
    }
    
    /**
     * Gets the current size of the worker pool.
     * 
     * @return pool size
     */
    public int getPoolSize() {
        return workerPool.getPoolSize();
    }
    
    /**
     * Gets statistics about task execution.
     * 
     * @return string representation of statistics
     */
    public String getStatistics() {
        return String.format("Bundles: %d, Active Workers: %d, Pool Size: %d, Total Tasks: %d, Completed: %d",
                           bundles.size(),
                           workerPool.getActiveCount(),
                           workerPool.getPoolSize(),
                           workerPool.getTaskCount(),
                           workerPool.getCompletedTaskCount());
    }
}
