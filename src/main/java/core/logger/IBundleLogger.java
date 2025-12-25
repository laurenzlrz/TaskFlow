package core.logger;

import core.nodes.IPipelineNode;
import core.run_bundle.LoggingRunBundle;

/**
 * Interface for logging the state and resource usage of pipeline nodes.
 *
 * @param <N> the type of pipeline node
 */
public interface IBundleLogger<N extends IPipelineNode> {

    /**
     * Begins logging for the given node and bundle.
     *
     * @param node the pipeline node
     * @param bundle the logging run bundle
     */
    void begin(N node, LoggingRunBundle<N, ?, ?> bundle);

    /**
     * Ends logging for the given node and bundle.
     *
     * @param node the pipeline node
     * @param bundle the logging run bundle
     */
    void end(N node, LoggingRunBundle<N, ?, ?> bundle);

}