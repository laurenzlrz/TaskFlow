package core.multithreading;

/**
 * Interface for a worker pool that manages thread parking and unparking.
 * This interface defines a pure parking and signaling mechanism without
 * any knowledge of bundles, DAGs, or higher-level task orchestration.
 */
public interface IWorkerPool {
    
    /**
     * Parks (puts to sleep) the calling thread.
     * The thread will remain parked until it is explicitly unparked
     * by another thread calling unparkWorkers().
     * 
     * This method blocks the calling thread.
     */
    void parkWorker();
    
    /**
     * Unparks (wakes up) exactly the specified number of threads.
     * If fewer than 'count' threads are currently parked, all parked
     * threads will be awakened.
     * 
     * @param count the number of threads to unpark
     */
    void unparkWorkers(int count);
}