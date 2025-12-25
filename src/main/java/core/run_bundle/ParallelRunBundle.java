package core.run_bundle;

import core.logger.BundleLogger;
import core.logger.IBundleLogger;
import core.nodes.IPipelineNode;
import core.multithreading.IWorkerPool;
import core.graph_representation.IRepresentation;
import core.protocol.Executable;
import core.protocol.RepresentationExecutable;
import def_elements.DebugLog;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Represents a parallel run bundle in a pipeline node.
 *
 * @param <N> the type of the pipeline node.
 * @param <R> the type of the representation.
 * @param <B> the type of the bundle logger.
 */
public class ParallelRunBundle<N extends IPipelineNode, R extends IRepresentation<N>, B extends BundleLogger<N>>
        extends LoggingRunBundle<N,R,B>{

    protected IWorkerPool workerPool;
    protected int numWorkers;
    protected ExecutorService executorService;
    protected Executable<N> executable;

    /**
     * Constructs a ParallelRunBundle with the specified representation, bundle logger, and worker pool.
     *
     * @param representation the representation of the pipeline node.
     * @param bundleLogger the logger for the bundle.
     * @param workerPool the worker pool for parallel execution.
     * @param numWorkers the number of worker threads
     */
    public ParallelRunBundle(R representation, IBundleLogger<N> bundleLogger, IWorkerPool workerPool, int numWorkers) {
        super(representation, bundleLogger);
        this.workerPool = workerPool;
        this.numWorkers = numWorkers;
        this.executorService = Executors.newFixedThreadPool(numWorkers);
        this.executable = new RepresentationExecutable<>(representation, this);
    }

    /**
     * Proceeds with the execution of the run bundle in parallel.
     */
    public void proceed() {
        this.abort = false;
        
        // Submit worker tasks
        for (int i = 0; i < this.numWorkers; i++) {
            executorService.submit(() -> {
                try {
                    this.proceedAll();
                } catch (Exception e) {
                    DebugLog.logThread("Worker thread exception: " + e.getMessage());
                    throw new RuntimeException(e);
                }
            });
        }
        
        // Also execute in current thread
        try {
            this.proceedAll();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        
        // Shutdown and wait for completion
        executorService.shutdown();
        try {
            executorService.awaitTermination(1, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    /**
     * Proceeds with the execution of all nodes in the run bundle.
     *
     * @throws InterruptedException if the execution is interrupted.
     */
    protected void proceedAll() throws InterruptedException {
        DebugLog.logThread("proceedAll");
        
        while (true) {
            if (this.abort) {
                DebugLog.logThread("Abort detected, notifying workers");
                this.workerPool.unparkWorkers(this.numWorkers);
                this.after_abortion();
                return;
            }

            if (this.representation.areAllNodesFinished()) {
                DebugLog.logThread("All nodes finished");
                this.afterFinish();
                this.workerPool.unparkWorkers(this.numWorkers);
                return;
            }

            if (!this.representation.areNodesRunning() && !this.representation.areNodesFree()) {
                DebugLog.logThread("No nodes running, no nodes free, exiting");
                this.workerPool.unparkWorkers(this.numWorkers);
                return;
            }

            this.proceedOneThreadAllNodes();
        }
    }

    /**
     * Proceeds with the execution of one thread for all nodes.
     *
     * @throws InterruptedException if the execution is interrupted.
     */
    protected void proceedOneThreadAllNodes() throws InterruptedException {
        
        N node = executable.checkIn();

        if (node == null) {
            DebugLog.logThread("Node was null, parking worker");
            // No node available, park the worker
            this.workerPool.parkWorker();
            
            // After waking up, check if we should continue
            if (this.representation.areAllNodesFinished() || this.abort) {
                return;
            }
            
            // Try again after being woken up
            node = executable.checkIn();
        }

        if (node == null) {
            DebugLog.logThread("Node is still null after wake up, returning");
            return;
        }

        DebugLog.logThread("proceedOneThreadOneNode: " + node);
        this.proceedOneThreadOneNode(node);
        DebugLog.logThread("proceedOneThreadOneNode finished: " + node);
        
        // Note: proceedOneThreadOneNode already calls representation.notifyFinished(node)
        // so we don't need to call executable.checkOut(node) here
        
        int availableCount = executable.getAvailableTaskCount();
        if (availableCount > 0) {
            DebugLog.logThread("Waking up workers, available count: " + availableCount);
            this.workerPool.unparkWorkers(Math.min(availableCount, this.numWorkers - 1));
        }
        
        DebugLog.logThread("Proceeded: " + node);
    }
}