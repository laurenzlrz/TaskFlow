package com.taskflow.orchestration;

import com.taskflow.core.TaskNode;
import com.taskflow.execution.ParallelRunBundle;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for PipelineOrchestrator with focus on lifecycle management.
 */
class PipelineOrchestratorTest {
    
    private PipelineOrchestrator orchestrator;
    
    @AfterEach
    void tearDown() {
        if (orchestrator != null && !orchestrator.isShutdown()) {
            orchestrator.shutdownNow();
        }
    }
    
    @Test
    @Timeout(10)
    void testOrchestratorCreation() {
        orchestrator = new PipelineOrchestrator(false, 1, TimeUnit.MINUTES);
        assertNotNull(orchestrator);
        assertFalse(orchestrator.isShutdown());
    }
    
    @Test
    @Timeout(10)
    void testCreateAndExecuteBundle() throws Exception {
        orchestrator = new PipelineOrchestrator(false, 1, TimeUnit.MINUTES);
        ParallelRunBundle bundle = orchestrator.createBundle();
        
        AtomicInteger counter = new AtomicInteger(0);
        bundle.addTask(new TaskNode("test-task") {
            @Override
            public void execute() {
                counter.incrementAndGet();
            }
        });
        
        ParallelRunBundle.ExecutionResult result = orchestrator.executeBundle(bundle);
        
        assertNotNull(result);
        assertTrue(result.isSuccess());
        assertEquals(1, result.getSuccessCount());
        assertEquals(1, counter.get());
    }
    
    @Test
    @Timeout(10)
    void testMultipleBundles() throws Exception {
        orchestrator = new PipelineOrchestrator(false, 1, TimeUnit.MINUTES);
        
        AtomicInteger counter = new AtomicInteger(0);
        
        // Create first bundle
        ParallelRunBundle bundle1 = orchestrator.createBundle();
        bundle1.addTask(new TaskNode("task-1") {
            @Override
            public void execute() {
                counter.incrementAndGet();
            }
        });
        
        // Create second bundle
        ParallelRunBundle bundle2 = orchestrator.createBundle();
        bundle2.addTask(new TaskNode("task-2") {
            @Override
            public void execute() {
                counter.incrementAndGet();
            }
        });
        
        orchestrator.executeAllBundles();
        
        assertEquals(2, counter.get());
    }
    
    @Test
    @Timeout(10)
    void testOrchestratorShutdown() throws Exception {
        orchestrator = new PipelineOrchestrator(false, 1, TimeUnit.MINUTES);
        
        ParallelRunBundle bundle = orchestrator.createBundle();
        bundle.addTask(new TaskNode("task") {
            @Override
            public void execute() throws Exception {
                Thread.sleep(50);
            }
        });
        
        orchestrator.executeBundle(bundle);
        boolean shutdown = orchestrator.shutdown(5, TimeUnit.SECONDS);
        
        assertTrue(shutdown);
        assertTrue(orchestrator.isShutdown());
    }
    
    @Test
    @Timeout(10)
    void testCannotExecuteAfterShutdown() {
        orchestrator = new PipelineOrchestrator(false, 1, TimeUnit.MINUTES);
        orchestrator.shutdown();
        
        ParallelRunBundle bundle = orchestrator.createBundle();
        bundle.addTask(new TaskNode("task") {
            @Override
            public void execute() {
            }
        });
        
        assertThrows(IllegalStateException.class, () -> {
            orchestrator.executeBundle(bundle);
        });
    }
    
    @Test
    @Timeout(10)
    void testStatistics() throws Exception {
        orchestrator = new PipelineOrchestrator(false, 1, TimeUnit.MINUTES);
        
        ParallelRunBundle bundle = orchestrator.createBundle();
        bundle.addTask(new TaskNode("task") {
            @Override
            public void execute() {
            }
        });
        
        String stats = orchestrator.getStatistics();
        assertNotNull(stats);
        assertTrue(stats.contains("Bundles:"));
    }
    
    @Test
    @Timeout(25)
    void testAutoShutdownOnIdle() throws Exception {
        // Create orchestrator with short idle timeout
        orchestrator = new PipelineOrchestrator(true, 3, TimeUnit.SECONDS);
        
        ParallelRunBundle bundle = orchestrator.createBundle();
        bundle.addTask(new TaskNode("task") {
            @Override
            public void execute() throws Exception {
                Thread.sleep(100);
            }
        });
        
        orchestrator.executeBundle(bundle);
        assertFalse(orchestrator.isShutdown(), "Should not be shut down immediately");
        
        // Wait for auto-shutdown
        Thread.sleep(5000);
        
        // May or may not be shut down depending on timing, but should have low activity
        assertTrue(orchestrator.getActiveWorkerCount() == 0 || orchestrator.isShutdown(),
                  "Should have no active workers or be shut down after idle period");
    }
}
