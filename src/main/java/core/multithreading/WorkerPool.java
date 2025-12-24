package core.multithreading;

import def_elements.DebugLog;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Class representing a worker pool for executing tasks concurrently.
 */
public class WorkerPool implements IWorkerPool {

    /**
     * The maximum number of lock tries.
     */
    public static final int MAX_LOCK_TRIES = 1000;

    /**
     * The executor service for managing worker threads.
     */
    protected ExecutorService executorService;

    /**
     * The number of workers in the pool.
     */
    protected int numWorkers;

    /**
     * The maximum number of lock tries.
     */
    protected int maxLockTries;

    /**
     * Constructs a new WorkerPool with the specified number of workers.
     *
     * @param numWorkers the number of workers in the pool
     */
    public WorkerPool(int numWorkers) {
        this.numWorkers = numWorkers;
        this.executorService = Executors.newFixedThreadPool(this.numWorkers);
        this.maxLockTries = MAX_LOCK_TRIES;
    }

    /**
     * Executes the same task for all workers in the pool.
     *
     * @param task the task to be executed
     */
    @Override
    public void doSameTaskForAll(Runnable task) {
        for (int i = 0; i < this.numWorkers; i++) {
            this.executorService.submit(task);
        }
        task.run();
    }

    /**
     * Waits until a specified condition is met, then performs an action.
     *
     * @param checkCondition the condition to be checked
     * @param changeAction the action to be performed when the condition is met
     * @throws InterruptedException if the current thread is interrupted while waiting
     */
    public void waitIfCondition(Callable<Boolean> checkCondition, Runnable changeAction) throws InterruptedException {

        Boolean con;
        int tries = 0;

        synchronized (this) {

            while (tries < this.maxLockTries) {
                tries++;

                changeAction.run();
                try {
                    con = checkCondition.call();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

                if (con) {
                    DebugLog.logThread("Condition met, waking up, tries: " + tries);
                    break;
                }
                DebugLog.logThread("Going to wait, tries: " + tries);
                this.wait();
            }
        }
    }

    /**
     * Notifies all workers in the pool.
     */
    public void workerPoolNotifyAll() {
        synchronized (this) {
            DebugLog.logThread("Notifying all");
            this.notifyAll();
        }
    }

    public void notifyOne() {
        synchronized (this) {
            DebugLog.logThread("Notifying one");
            this.notify();
        }
    }
}