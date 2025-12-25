package core.multithreading;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for WorkerPool implementation.
 */
class WorkerPoolTest {
    
    @Test
    @Timeout(5)
    void testParkAndUnpark() throws InterruptedException {
        WorkerPool pool = new WorkerPool();
        CountDownLatch parked = new CountDownLatch(1);
        CountDownLatch unparked = new CountDownLatch(1);
        
        Thread worker = new Thread(() -> {
            parked.countDown();
            pool.parkWorker();
            unparked.countDown();
        });
        
        worker.start();
        
        // Wait for worker to park
        assertTrue(parked.await(1, TimeUnit.SECONDS));
        Thread.sleep(100); // Give it time to actually park
        
        // Verify worker is parked
        assertEquals(1, pool.getParkedWorkerCount());
        
        // Unpark the worker
        pool.unparkWorkers(1);
        
        // Wait for worker to complete
        assertTrue(unparked.await(1, TimeUnit.SECONDS));
        worker.join(1000);
        
        // Verify worker is no longer parked
        assertEquals(0, pool.getParkedWorkerCount());
    }
    
    @Test
    @Timeout(5)
    void testUnparkMultipleWorkers() throws InterruptedException {
        WorkerPool pool = new WorkerPool();
        int workerCount = 5;
        CountDownLatch parked = new CountDownLatch(workerCount);
        CountDownLatch unparked = new CountDownLatch(workerCount);
        
        // Start multiple workers
        for (int i = 0; i < workerCount; i++) {
            new Thread(() -> {
                parked.countDown();
                pool.parkWorker();
                unparked.countDown();
            }).start();
        }
        
        // Wait for all workers to park
        assertTrue(parked.await(1, TimeUnit.SECONDS));
        Thread.sleep(100); // Give them time to actually park
        
        // Verify all workers are parked
        assertEquals(workerCount, pool.getParkedWorkerCount());
        
        // Unpark all workers
        pool.unparkWorkers(workerCount);
        
        // Wait for all workers to complete
        assertTrue(unparked.await(1, TimeUnit.SECONDS));
        
        // Give threads time to fully exit parkWorker
        Thread.sleep(100);
        
        // Verify all workers are unparked
        assertEquals(0, pool.getParkedWorkerCount());
    }
    
    @Test
    @Timeout(5)
    void testUnparkFewerThanParked() throws InterruptedException {
        WorkerPool pool = new WorkerPool();
        int workerCount = 5;
        int unparkCount = 2;
        CountDownLatch parked = new CountDownLatch(workerCount);
        AtomicInteger unparked = new AtomicInteger(0);
        
        // Start multiple workers
        for (int i = 0; i < workerCount; i++) {
            new Thread(() -> {
                parked.countDown();
                pool.parkWorker();
                unparked.incrementAndGet();
            }).start();
        }
        
        // Wait for all workers to park
        assertTrue(parked.await(1, TimeUnit.SECONDS));
        Thread.sleep(100); // Give them time to actually park
        
        // Verify all workers are parked
        assertEquals(workerCount, pool.getParkedWorkerCount());
        
        // Unpark only some workers
        pool.unparkWorkers(unparkCount);
        
        // Wait a bit for unparked workers to complete
        Thread.sleep(200);
        
        // Verify correct number of workers are still parked
        assertEquals(workerCount - unparkCount, pool.getParkedWorkerCount());
        assertEquals(unparkCount, unparked.get());
    }
    
    @Test
    @Timeout(5)
    void testUnparkWithZeroCount() throws InterruptedException {
        WorkerPool pool = new WorkerPool();
        CountDownLatch parked = new CountDownLatch(1);
        
        Thread worker = new Thread(() -> {
            parked.countDown();
            pool.parkWorker();
        });
        
        worker.start();
        
        // Wait for worker to park
        assertTrue(parked.await(1, TimeUnit.SECONDS));
        Thread.sleep(100);
        
        // Verify worker is parked
        assertEquals(1, pool.getParkedWorkerCount());
        
        // Try to unpark with zero count (should do nothing)
        pool.unparkWorkers(0);
        Thread.sleep(100);
        
        // Verify worker is still parked
        assertEquals(1, pool.getParkedWorkerCount());
        
        // Clean up
        pool.unparkWorkers(1);
        worker.join(1000);
    }
    
    @Test
    @Timeout(5)
    void testUnparkMoreThanParked() throws InterruptedException {
        WorkerPool pool = new WorkerPool();
        int workerCount = 3;
        CountDownLatch parked = new CountDownLatch(workerCount);
        CountDownLatch unparked = new CountDownLatch(workerCount);
        
        // Start workers
        for (int i = 0; i < workerCount; i++) {
            new Thread(() -> {
                parked.countDown();
                pool.parkWorker();
                unparked.countDown();
            }).start();
        }
        
        // Wait for all workers to park
        assertTrue(parked.await(1, TimeUnit.SECONDS));
        Thread.sleep(100);
        
        // Try to unpark more workers than are parked
        pool.unparkWorkers(workerCount + 10);
        
        // All workers should be unparked
        assertTrue(unparked.await(1, TimeUnit.SECONDS));
        Thread.sleep(100);
        assertEquals(0, pool.getParkedWorkerCount());
    }
    
    @Test
    void testUnparkWithNegativeCount() {
        WorkerPool pool = new WorkerPool();
        
        // Should handle negative counts gracefully (do nothing)
        assertDoesNotThrow(() -> pool.unparkWorkers(-5));
    }
    
    @Test
    @Timeout(5)
    void testGetParkedWorkerCount() throws InterruptedException {
        WorkerPool pool = new WorkerPool();
        
        // Initially, no workers are parked
        assertEquals(0, pool.getParkedWorkerCount());
        
        CountDownLatch parked = new CountDownLatch(1);
        Thread worker = new Thread(() -> {
            parked.countDown();
            pool.parkWorker();
        });
        
        worker.start();
        assertTrue(parked.await(1, TimeUnit.SECONDS));
        Thread.sleep(100);
        
        // One worker should be parked
        assertEquals(1, pool.getParkedWorkerCount());
        
        // Clean up
        pool.unparkWorkers(1);
        worker.join(1000);
    }
}
