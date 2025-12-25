package def_elements;

import core.multithreading.IWorkerPool;
import core.run_bundle.ParallelRunBundle;

/**
 * Default implementation of a parallel run bundle for DefPipelineNode.
 */
public class DefRunBundle extends ParallelRunBundle<DefPipelineNode, DefRepresentation, DefBundleLogger> {

    /**
     * Constructs a DefRunBundle with the specified representation, logger, and worker pool.
     *
     * @param representation the representation of the pipeline node.
     * @param logger the logger for the bundle.
     * @param workerPool the worker pool for parallel execution.
     * @param numWorkers the number of worker threads
     */
    public DefRunBundle(DefRepresentation representation, DefBundleLogger logger, IWorkerPool workerPool, int numWorkers) {
        super(representation, logger, workerPool, numWorkers);
    }
}