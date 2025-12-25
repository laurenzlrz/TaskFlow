package core.multithreading;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Implementation of IWorkerPool using ReentrantLock and Condition.
 * This class provides efficient thread parking and unparking without
 * busy-waiting or spin-locks.
 * 
 * Thread-safe implementation that uses condition variables for 
 * efficient thread synchronization.
 */
public class WorkerPool implements IWorkerPool {
    
    private final ReentrantLock lock;
    private final Condition condition;
    private int parkedWorkers;
    
    /**
     * Creates a new WorkerPool instance.
     */
    public WorkerPool() {
        this.lock = new ReentrantLock();
        this.condition = lock.newCondition();
        this.parkedWorkers = 0;
    }
    
    /**
     * Parks (puts to sleep) the calling thread.
     * The thread will remain parked until it is explicitly unparked
     * by another thread calling unparkWorkers().
     * 
     * This method blocks the calling thread.
     */
    @Override
    public void parkWorker() {
        lock.lock();
        try {
            parkedWorkers++;
            condition.await();
            parkedWorkers--;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Thread was interrupted while parked", e);
        } finally {
            lock.unlock();
        }
    }
    
    /**
     * Unparks (wakes up) exactly the specified number of threads.
     * If fewer than 'count' threads are currently parked, all parked
     * threads will be awakened.
     * 
     * @param count the number of threads to unpark
     */
    @Override
    public void unparkWorkers(int count) {
        if (count <= 0) {
            return;
        }
        
        lock.lock();
        try {
            int toUnpark = Math.min(count, parkedWorkers);
            for (int i = 0; i < toUnpark; i++) {
                condition.signal();
            }
        } finally {
            lock.unlock();
        }
    }
    
    /**
     * Returns the number of currently parked workers.
     * This method is useful for monitoring and testing purposes.
     * 
     * @return the number of parked workers
     */
    public int getParkedWorkerCount() {
        lock.lock();
        try {
            return parkedWorkers;
        } finally {
            lock.unlock();
        }
    }
}