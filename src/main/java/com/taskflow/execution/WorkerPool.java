package com.taskflow.execution;

import com.taskflow.logging.LogMessages;
import com.taskflow.logging.TaskFlowLogger;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Manages a pool of worker threads for executing tasks.
 * Uses ThreadPoolExecutor with keepAliveTime for efficient resource management.
 * Threads are automatically terminated after being idle for a specified duration.
 */
public class WorkerPool {
    private final TaskFlowLogger logger;
    
    private final ThreadPoolExecutor executor;
    private final long keepAliveTime;
    private final TimeUnit keepAliveTimeUnit;
    private final AtomicBoolean isShutdown = new AtomicBoolean(false);
    private final ScheduledExecutorService idleCheckExecutor;
    private ScheduledFuture<?> idleCheckTask;
    
    /**
     * Creates a WorkerPool with specified configuration.
     * 
     * @param corePoolSize the number of threads to keep in the pool, even if they are idle
     * @param maxPoolSize the maximum number of threads to allow in the pool
     * @param keepAliveTime when the number of threads is greater than the core,
     *                      this is the maximum time that excess idle threads will wait
     *                      for new tasks before terminating
     * @param unit the time unit for the keepAliveTime argument
     * @param logger the logger to use for this worker pool
     */
    public WorkerPool(int corePoolSize, int maxPoolSize, long keepAliveTime, TimeUnit unit, TaskFlowLogger logger) {
        this.keepAliveTime = keepAliveTime;
        this.keepAliveTimeUnit = unit;
        this.logger = logger;
        
        // Create ThreadPoolExecutor with a work queue
        this.executor = new ThreadPoolExecutor(
            corePoolSize,
            maxPoolSize,
            keepAliveTime,
            unit,
            new LinkedBlockingQueue<>(),
            new ThreadFactory() {
                private int counter = 0;
                @Override
                public Thread newThread(Runnable r) {
                    Thread t = new Thread(r);
                    t.setName("TaskFlow-Worker-" + counter++);
                    t.setDaemon(false);
                    return t;
                }
            }
        );
        
        // Allow core threads to timeout as well for better resource management
        this.executor.allowCoreThreadTimeOut(true);
        
        // Create a scheduled executor for idle checks
        this.idleCheckExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r);
            t.setName("TaskFlow-IdleCheck");
            t.setDaemon(true);
            return t;
        });
        
        logger.info(LogMessages.WORKER_POOL_CREATED, corePoolSize, maxPoolSize, keepAliveTime, unit);
    }
    
    /**
     * Creates a WorkerPool with specified configuration and default logger.
     * 
     * @param corePoolSize the number of threads to keep in the pool
     * @param maxPoolSize the maximum number of threads to allow in the pool
     * @param keepAliveTime thread keep-alive time
     * @param unit the time unit for the keepAliveTime argument
     */
    public WorkerPool(int corePoolSize, int maxPoolSize, long keepAliveTime, TimeUnit unit) {
        this(corePoolSize, maxPoolSize, keepAliveTime, unit, TaskFlowLogger.forClass(WorkerPool.class));
    }
    
    /**
     * Creates a WorkerPool with default configuration.
     * Uses Runtime.getRuntime().availableProcessors() as core and max pool size.
     * Default keepAliveTime is 60 seconds.
     */
    public WorkerPool() {
        this(Runtime.getRuntime().availableProcessors(),
             Runtime.getRuntime().availableProcessors() * 2,
             60L,
             TimeUnit.SECONDS);
    }
    
    /**
     * Submits a task for execution.
     * 
     * @param task the task to execute
     * @return a Future representing pending completion of the task
     * @throws RejectedExecutionException if the task cannot be scheduled for execution
     */
    public Future<?> submit(Runnable task) {
        if (isShutdown.get()) {
            throw new RejectedExecutionException(LogMessages.WORKER_POOL_SHUTDOWN_REJECTED);
        }
        return executor.submit(task);
    }
    
    /**
     * Submits a callable task for execution.
     * 
     * @param task the task to execute
     * @param <T> the type of the task's result
     * @return a Future representing pending completion of the task
     * @throws RejectedExecutionException if the task cannot be scheduled for execution
     */
    public <T> Future<T> submit(Callable<T> task) {
        if (isShutdown.get()) {
            throw new RejectedExecutionException(LogMessages.WORKER_POOL_SHUTDOWN_REJECTED);
        }
        return executor.submit(task);
    }
    
    /**
     * Starts monitoring for idle state and automatically shuts down after specified duration.
     * 
     * @param idleDuration the duration of idle time before automatic shutdown
     * @param unit the time unit for the idle duration
     */
    public void enableAutoShutdownOnIdle(long idleDuration, TimeUnit unit) {
        if (idleCheckTask != null) {
            idleCheckTask.cancel(false);
        }
        
        idleCheckTask = idleCheckExecutor.scheduleAtFixedRate(() -> {
            // Check if all submitted tasks have been completed (more atomic than checking active count and queue separately)
            if (executor.getTaskCount() == executor.getCompletedTaskCount() && executor.getTaskCount() > 0) {
                long idleTime = unit.toMillis(idleDuration);
                logger.info(LogMessages.WORKER_POOL_IDLE, idleTime);
                shutdownGracefully();
            }
        }, idleDuration, idleDuration, unit);
        
        logger.info(LogMessages.AUTO_SHUTDOWN_ENABLED, idleDuration, unit);
    }
    
    /**
     * Disables automatic shutdown on idle.
     */
    public void disableAutoShutdownOnIdle() {
        if (idleCheckTask != null) {
            idleCheckTask.cancel(false);
            idleCheckTask = null;
            logger.info(LogMessages.AUTO_SHUTDOWN_DISABLED);
        }
    }
    
    /**
     * Initiates an orderly shutdown in which previously submitted tasks are executed,
     * but no new tasks will be accepted.
     */
    public void shutdown() {
        if (isShutdown.compareAndSet(false, true)) {
            logger.info(LogMessages.WORKER_POOL_SHUTTING_DOWN);
            disableAutoShutdownOnIdle();
            executor.shutdown();
            idleCheckExecutor.shutdown();
        }
    }
    
    /**
     * Attempts to stop all actively executing tasks, halts the processing of waiting tasks,
     * and returns a list of the tasks that were awaiting execution.
     * 
     * @return list of tasks that never commenced execution
     */
    public java.util.List<Runnable> shutdownNow() {
        if (isShutdown.compareAndSet(false, true)) {
            logger.warning(LogMessages.WORKER_POOL_FORCE_SHUTDOWN);
            disableAutoShutdownOnIdle();
            idleCheckExecutor.shutdownNow();
            return executor.shutdownNow();
        }
        return java.util.Collections.emptyList();
    }
    
    /**
     * Initiates a graceful shutdown and waits for all tasks to complete.
     * 
     * @return true if the pool terminated, false if timeout elapsed before termination
     */
    public boolean shutdownGracefully() {
        return shutdownGracefully(30, TimeUnit.SECONDS);
    }
    
    /**
     * Initiates a graceful shutdown and waits for all tasks to complete.
     * 
     * @param timeout the maximum time to wait
     * @param unit the time unit of the timeout argument
     * @return true if the pool terminated, false if timeout elapsed before termination
     */
    public boolean shutdownGracefully(long timeout, TimeUnit unit) {
        shutdown();
        try {
            if (!executor.awaitTermination(timeout, unit)) {
                logger.warning(LogMessages.WORKER_POOL_SHUTDOWN_TIMEOUT);
                shutdownNow();
                // Use a shorter timeout for forced shutdown (half of original)
                long remainingTimeout = unit.toMillis(timeout) / 2;
                return executor.awaitTermination(remainingTimeout, TimeUnit.MILLISECONDS);
            }
            logger.info(LogMessages.WORKER_POOL_SHUTDOWN_SUCCESS);
            return true;
        } catch (InterruptedException e) {
            logger.warning(LogMessages.WORKER_POOL_SHUTDOWN_INTERRUPTED);
            shutdownNow();
            Thread.currentThread().interrupt();
            return false;
        }
    }
    
    /**
     * Returns true if this pool has been shut down.
     * 
     * @return true if shut down
     */
    public boolean isShutdown() {
        return isShutdown.get();
    }
    
    /**
     * Returns true if all tasks have completed following shutdown.
     * 
     * @return true if terminated
     */
    public boolean isTerminated() {
        return executor.isTerminated();
    }
    
    /**
     * Gets the current number of active threads.
     * 
     * @return number of active threads
     */
    public int getActiveCount() {
        return executor.getActiveCount();
    }
    
    /**
     * Gets the approximate number of threads that are actively executing tasks.
     * 
     * @return number of threads executing tasks
     */
    public long getTaskCount() {
        return executor.getTaskCount();
    }
    
    /**
     * Gets the approximate number of tasks that have completed execution.
     * 
     * @return number of completed tasks
     */
    public long getCompletedTaskCount() {
        return executor.getCompletedTaskCount();
    }
    
    /**
     * Gets the current pool size.
     * 
     * @return current number of threads in the pool
     */
    public int getPoolSize() {
        return executor.getPoolSize();
    }
}
