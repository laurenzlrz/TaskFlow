package com.taskflow.execution;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

/**
 * Manages a pool of worker threads for executing tasks.
 * Uses ThreadPoolExecutor with keepAliveTime for efficient resource management.
 * Threads are automatically terminated after being idle for a specified duration.
 */
public class WorkerPool {
    private static final Logger LOGGER = Logger.getLogger(WorkerPool.class.getName());
    
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
     */
    public WorkerPool(int corePoolSize, int maxPoolSize, long keepAliveTime, TimeUnit unit) {
        this.keepAliveTime = keepAliveTime;
        this.keepAliveTimeUnit = unit;
        
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
        
        LOGGER.info(String.format("WorkerPool created with %d core threads, %d max threads, " +
                                 "%d %s keepAliveTime", 
                                 corePoolSize, maxPoolSize, keepAliveTime, unit));
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
            throw new RejectedExecutionException("WorkerPool has been shut down");
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
            throw new RejectedExecutionException("WorkerPool has been shut down");
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
            if (executor.getActiveCount() == 0 && executor.getQueue().isEmpty()) {
                long idleTime = unit.toMillis(idleDuration);
                LOGGER.info(String.format("WorkerPool idle for %d ms, initiating shutdown", idleTime));
                shutdownGracefully();
            }
        }, idleDuration, idleDuration, unit);
        
        LOGGER.info(String.format("Auto-shutdown on idle enabled: %d %s", idleDuration, unit));
    }
    
    /**
     * Disables automatic shutdown on idle.
     */
    public void disableAutoShutdownOnIdle() {
        if (idleCheckTask != null) {
            idleCheckTask.cancel(false);
            idleCheckTask = null;
            LOGGER.info("Auto-shutdown on idle disabled");
        }
    }
    
    /**
     * Initiates an orderly shutdown in which previously submitted tasks are executed,
     * but no new tasks will be accepted.
     */
    public void shutdown() {
        if (isShutdown.compareAndSet(false, true)) {
            LOGGER.info("Shutting down WorkerPool");
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
            LOGGER.warning("Forcing immediate shutdown of WorkerPool");
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
                LOGGER.warning("WorkerPool did not terminate in time, forcing shutdown");
                shutdownNow();
                return executor.awaitTermination(timeout, unit);
            }
            LOGGER.info("WorkerPool shutdown completed successfully");
            return true;
        } catch (InterruptedException e) {
            LOGGER.warning("Shutdown interrupted, forcing immediate shutdown");
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
