package com.taskflow;

import com.taskflow.core.TaskNode;
import com.taskflow.execution.ParallelRunBundle;
import com.taskflow.orchestration.PipelineOrchestrator;

import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Example demonstrating the usage of TaskFlow with optimized thread management.
 * Shows how threads are efficiently managed with keepAliveTime and auto-shutdown.
 */
public class Example {
    private static final Logger LOGGER = Logger.getLogger(Example.class.getName());
    
    public static void main(String[] args) {
        // Create orchestrator with auto-shutdown after 10 seconds of inactivity
        PipelineOrchestrator orchestrator = new PipelineOrchestrator(
            true,  // enable auto-shutdown
            10L,   // 10 seconds idle timeout
            TimeUnit.SECONDS
        );
        
        try {
            // Example 1: Simple linear pipeline
            LOGGER.info("=== Example 1: Simple Linear Pipeline ===");
            runLinearPipeline(orchestrator);
            
            // Example 2: DAG with dependencies
            LOGGER.info("\n=== Example 2: DAG Pipeline with Dependencies ===");
            runDagPipeline(orchestrator);
            
            // Show statistics
            LOGGER.info("\n=== Statistics ===");
            LOGGER.info(orchestrator.getStatistics());
            
            // Wait a bit to show that threads become idle
            LOGGER.info("\nWaiting 5 seconds to observe thread pool behavior...");
            Thread.sleep(5000);
            LOGGER.info("Active workers: " + orchestrator.getActiveWorkerCount());
            LOGGER.info("Pool size: " + orchestrator.getPoolSize());
            
        } catch (Exception e) {
            LOGGER.severe("Error executing pipeline: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // Graceful shutdown
            LOGGER.info("\nShutting down orchestrator...");
            orchestrator.shutdown(30, TimeUnit.SECONDS);
            LOGGER.info("Shutdown complete");
        }
    }
    
    /**
     * Runs a simple linear pipeline with independent tasks.
     */
    private static void runLinearPipeline(PipelineOrchestrator orchestrator) throws Exception {
        ParallelRunBundle bundle = orchestrator.createBundle();
        
        // Create 5 independent tasks
        for (int i = 1; i <= 5; i++) {
            final int taskNum = i;
            bundle.addTask(new TaskNode("task-" + i) {
                @Override
                public void execute() throws Exception {
                    LOGGER.info(String.format("Task %d starting on thread %s", 
                                             taskNum, Thread.currentThread().getName()));
                    Thread.sleep(100); // Simulate work
                    LOGGER.info(String.format("Task %d completed", taskNum));
                }
            });
        }
        
        ParallelRunBundle.ExecutionResult result = orchestrator.executeBundle(bundle);
        LOGGER.info("Result: " + result);
    }
    
    /**
     * Runs a DAG pipeline where tasks have dependencies.
     */
    private static void runDagPipeline(PipelineOrchestrator orchestrator) throws Exception {
        ParallelRunBundle bundle = orchestrator.createBundle();
        
        // Create a simple DAG:
        //     Task1
        //    /     \
        // Task2   Task3
        //    \     /
        //     Task4
        
        TaskNode task1 = new TaskNode("dag-task-1") {
            @Override
            public void execute() throws Exception {
                LOGGER.info("DAG Task 1 executing (root)");
                Thread.sleep(100);
            }
        };
        
        TaskNode task2 = new TaskNode("dag-task-2") {
            @Override
            public void execute() throws Exception {
                LOGGER.info("DAG Task 2 executing (depends on task 1)");
                Thread.sleep(100);
            }
        };
        task2.addDependency(task1);
        
        TaskNode task3 = new TaskNode("dag-task-3") {
            @Override
            public void execute() throws Exception {
                LOGGER.info("DAG Task 3 executing (depends on task 1)");
                Thread.sleep(100);
            }
        };
        task3.addDependency(task1);
        
        TaskNode task4 = new TaskNode("dag-task-4") {
            @Override
            public void execute() throws Exception {
                LOGGER.info("DAG Task 4 executing (depends on tasks 2 and 3)");
                Thread.sleep(100);
            }
        };
        task4.addDependency(task2);
        task4.addDependency(task3);
        
        bundle.addTask(task1);
        bundle.addTask(task2);
        bundle.addTask(task3);
        bundle.addTask(task4);
        
        ParallelRunBundle.ExecutionResult result = orchestrator.executeBundle(bundle);
        LOGGER.info("Result: " + result);
    }
}
