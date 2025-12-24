package core.run_bundle;

import core.logger.BundleLogger;
import core.logger.IBundleLogger;
import core.nodes.IPipelineNode;
import core.multithreading.IWorkerPool;
import core.graph_representation.IRepresentation;
import def_elements.DebugLog;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

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

    /**
     * Constructs a ParallelRunBundle with the specified representation, bundle logger, and worker pool.
     *
     * @param representation the representation of the pipeline node.
     * @param bundleLogger the logger for the bundle.
     * @param workerPool the worker pool for parallel execution.
     */
    public ParallelRunBundle(R representation, IBundleLogger<N> bundleLogger, IWorkerPool workerPool) {
        super(representation, bundleLogger);
        this.workerPool = workerPool;
    }

    /**
     * Proceeds with the execution of the run bundle in parallel.
     */
    public void proceed() {
        this.abort = false;
        Runnable runnable = () -> {
            try {
                this.proceedAll();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        };
        this.workerPool.doSameTaskForAll(runnable);
    }

    /**
     * Proceeds with the execution of all nodes in the run bundle.
     *
     * @throws InterruptedException if the execution is interrupted.
     */
    protected void proceedAll() throws InterruptedException {
        DebugLog.logThread("proceedAll");
        if (this.abort) {
            this.workerPool.workerPoolNotifyAll();
            this.after_abortion();
            return;
        }

        if (this.representation.areAllNodesFinished()) {
            this.afterFinish();
            this.workerPool.workerPoolNotifyAll();
            return;
        }

        if (!this.representation.areNodesRunning() && !this.representation.areNodesFree()) {
            DebugLog.logThread("No nodes running, no nodes free, returning");
            this.workerPool.workerPoolNotifyAll();
            return;
        }

        this.proceedOneThreadAllNodes();
        this.proceedAll();
    }

    /**
     * Proceeds with the execution of one thread for all nodes.
     *
     * @throws InterruptedException if the execution is interrupted.
     */
    protected void proceedOneThreadAllNodes() throws InterruptedException {
        
        N node = null;
        if (this.representation.areNodesAvailable()) {
            node = this.representation.takeNextNode(this);
        }

        if (node == null) {
            AtomicReference<N> atomicNode = new AtomicReference<>();

            DebugLog.logThread("Node was initially null, going into condition management");
            Runnable changeAction = () -> atomicNode.set(this.representation.takeNextNode(this));
            Callable<Boolean> checkCondition = () -> atomicNode.get() != null ||
                    this.representation.areAllNodesFinished();

            this.workerPool.waitIfCondition(checkCondition, changeAction);
            node = atomicNode.get();
        }

        if (node == null) {
            DebugLog.logThread("Node is still null after condition management, returning");
            return;
        }

        DebugLog.logThread("proceedOneThreadOneNode: " + node);
        this.proceedOneThreadOneNode(node);
        DebugLog.logThread("proceedOneThreadOneNode finished: " + node);
        if (this.representation.areNodesAvailable()) {
            this.workerPool.workerPoolNotifyAll();
        }
        DebugLog.logThread("Proceeded: " + node);
    }
}