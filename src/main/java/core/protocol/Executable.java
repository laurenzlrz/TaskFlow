package core.protocol;

/**
 * The generic bridge between business logic and multithreading.
 * This interface encapsulates the state of executable tasks (e.g., ParallelRepresentation)
 * and provides thread-safe methods for task coordination.
 * 
 * @param <T> the type of task that can be executed
 */
public interface Executable<T> {
    
    /**
     * Retrieves the next atomic task that is ready for execution.
     * Returns null if no task is currently available.
     * 
     * This method is thread-safe and can be called concurrently by multiple threads.
     * 
     * @return the next task to execute, or null if none available
     */
    T checkIn();
    
    /**
     * Reports the completion of a task and updates the internal state.
     * This method should be called after a task has finished executing.
     * 
     * This method is thread-safe and can be called concurrently by multiple threads.
     * 
     * @param task the task that has been completed
     */
    void checkOut(T task);
    
    /**
     * Returns the number of tasks that are currently ready for execution.
     * This count represents tasks that can be immediately retrieved via checkIn().
     * 
     * This method is thread-safe and can be called concurrently by multiple threads.
     * 
     * @return the number of available tasks
     */
    int getAvailableTaskCount();
}
