package core.multithreading;

import java.util.concurrent.Callable;

/**
 * Interface representing a worker pool for executing tasks concurrently.
 */
public interface IWorkerPool {

    /**
     * Executes the same task for all workers in the pool.
     *
     * @param task the task to be executed
     */
    void doSameTaskForAll(Runnable task);

    /**
     * Notifies all workers in the pool.
     *
     * @throws InterruptedException if the current thread is interrupted while waiting
     */
    void workerPoolNotifyAll() throws InterruptedException;

    /**
     * Waits until a specified condition is met, then performs an action.
     *
     * @param checkCondition the condition to be checked
     * @param actionCondition the action to be performed when the condition is met
     * @throws InterruptedException if the current thread is interrupted while waiting
     */
    void waitIfCondition(Callable<Boolean> checkCondition, Runnable actionCondition) throws InterruptedException;
}