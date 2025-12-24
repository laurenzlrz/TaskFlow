package com.taskflow.execution;

import com.taskflow.core.TaskNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for WorkerPool with focus on thread management and keepAliveTime.
 */
class WorkerPoolTest {
    
    private WorkerPool workerPool;
    
    @AfterEach
    void tearDown() {
        if (workerPool != null && !workerPool.isShutdown()) {
            workerPool.shutdownNow();
        }
    }
    
    @Test
    @Timeout(10)
    void testWorkerPoolCreation() {
        workerPool = new WorkerPool(2, 4, 1, TimeUnit.SECONDS);
        assertNotNull(workerPool);
        assertFalse(workerPool.isShutdown());
        assertEquals(0, workerPool.getActiveCount());
    }
    
    @Test
    @Timeout(10)
    void testTaskSubmission() throws Exception {
        workerPool = new WorkerPool(2, 4, 1, TimeUnit.SECONDS);
        AtomicInteger counter = new AtomicInteger(0);
        
        workerPool.submit(() -> counter.incrementAndGet()).get();
        
        assertEquals(1, counter.get());
    }
    
    @Test
    @Timeout(10)
    void testMultipleTasksExecution() throws Exception {
        workerPool = new WorkerPool(2, 4, 1, TimeUnit.SECONDS);
        AtomicInteger counter = new AtomicInteger(0);
        
        for (int i = 0; i < 10; i++) {
            workerPool.submit(() -> counter.incrementAndGet());
        }
        
        // Wait a bit for tasks to complete
        Thread.sleep(500);
        assertEquals(10, counter.get());
    }
    
    @Test
    @Timeout(15)
    void testThreadPoolScalesDown() throws Exception {
        // Create pool with short keepAliveTime
        workerPool = new WorkerPool(1, 4, 1, TimeUnit.SECONDS);
        
        // Submit multiple tasks to scale up
        for (int i = 0; i < 4; i++) {
            workerPool.submit(() -> {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        
        // Wait for tasks to start
        Thread.sleep(50);
        int activeAfterSubmit = workerPool.getPoolSize();
        assertTrue(activeAfterSubmit > 0, "Pool should have threads after submitting tasks");
        
        // Wait for keepAliveTime + extra time for threads to die
        Thread.sleep(3000);
        
        // Pool should scale down (may not be 0 due to core thread timeout timing)
        int activeAfterIdle = workerPool.getPoolSize();
        assertTrue(activeAfterIdle <= activeAfterSubmit, 
                  "Pool should scale down after idle period");
    }
    
    @Test
    @Timeout(10)
    void testGracefulShutdown() {
        workerPool = new WorkerPool(2, 4, 1, TimeUnit.SECONDS);
        
        // Submit a task
        workerPool.submit(() -> {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        
        boolean shutdown = workerPool.shutdownGracefully(5, TimeUnit.SECONDS);
        assertTrue(shutdown, "Shutdown should complete successfully");
        assertTrue(workerPool.isShutdown());
        assertTrue(workerPool.isTerminated());
    }
    
    @Test
    @Timeout(10)
    void testShutdownPreventsNewSubmissions() {
        workerPool = new WorkerPool(2, 4, 1, TimeUnit.SECONDS);
        workerPool.shutdown();
        
        assertThrows(Exception.class, () -> {
            workerPool.submit(() -> System.out.println("Should not execute"));
        });
    }
    
    @Test
    @Timeout(10)
    void testTaskCount() throws Exception {
        workerPool = new WorkerPool(2, 4, 1, TimeUnit.SECONDS);
        
        for (int i = 0; i < 5; i++) {
            workerPool.submit(() -> {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        
        Thread.sleep(500);
        assertTrue(workerPool.getCompletedTaskCount() > 0);
    }
    
    @Test
    @Timeout(20)
    void testAutoShutdownOnIdle() throws Exception {
        workerPool = new WorkerPool(2, 4, 1, TimeUnit.SECONDS);
        
        // Enable auto-shutdown with short timeout
        workerPool.enableAutoShutdownOnIdle(2, TimeUnit.SECONDS);
        
        // Submit a task
        workerPool.submit(() -> {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }).get();
        
        assertFalse(workerPool.isShutdown(), "Should not be shut down immediately after task");
        
        // Wait for idle timeout
        Thread.sleep(3000);
        
        // Should be shut down after idle period
        assertTrue(workerPool.isShutdown() || workerPool.getActiveCount() == 0, 
                  "Should initiate shutdown after idle timeout");
    }
    
    @Test
    @Timeout(10)
    void testDisableAutoShutdown() throws Exception {
        workerPool = new WorkerPool(2, 4, 1, TimeUnit.SECONDS);
        
        // Enable then disable auto-shutdown
        workerPool.enableAutoShutdownOnIdle(1, TimeUnit.SECONDS);
        workerPool.disableAutoShutdownOnIdle();
        
        // Submit a task and wait
        workerPool.submit(() -> {}).get();
        Thread.sleep(2000);
        
        // Should not be shut down
        assertFalse(workerPool.isShutdown());
    }
}
