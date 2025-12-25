package core.run_bundle;

import core.nodes.IPipelineNode;

/**
 * Interface representing a run bundle in a pipeline node.
 *
 * @param <N> the type of the pipeline node.
 */
public interface IRunBundle<N extends IPipelineNode> {

    /**
     * Proceeds with the execution of the run bundle.
     */
    void proceed();

    /**
     * Aborts the execution of the run bundle.
     */
    void abort();

}