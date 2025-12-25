package core.run_bundle;

import core.logger.BundleLogger;
import core.logger.IBundleLogger;
import core.nodes.IPipelineNode;
import core.graph_representation.IRepresentation;

/**
 * Represents a logging run bundle in a pipeline node.
 *
 * @param <N> the type of the pipeline node.
 * @param <R> the type of the representation.
 * @param <B> the type of the bundle logger.
 */
public class LoggingRunBundle<N extends IPipelineNode, R extends IRepresentation<N>, B extends BundleLogger<N>>
        extends RunBundle<N, R> {

    protected B loggingReference;

    private final IBundleLogger<N> logger;

    /**
     * Constructs a LoggingRunBundle with the specified representation and logger.
     *
     * @param representation the representation of the pipeline node.
     * @param logger the logger for the bundle.
     */
    public LoggingRunBundle(R representation, IBundleLogger<N> logger) {
        super(representation);
        this.logger = logger;
    }

    /**
     * Hook method called before signing in a node.
     *
     * @param node the pipeline node to sign in.
     */
    protected void fabric_before_sign_in(N node) {
        this.logger.begin(node, this);
        this.before_sign_in(node);
    }

    /**
     * Hook method called after signing out a node.
     *
     * @param node the pipeline node to sign out.
     */
    protected void fabric_after_sign_out(N node) {
        this.logger.end(node, this);
        this.after_sign_out(node);
    }

    /**
     * Sets the logging reference for the bundle.
     *
     * @param loggingReference the logging reference to set.
     */
    protected void setLoggingReference(B loggingReference) {
        this.loggingReference = loggingReference;
    }

}