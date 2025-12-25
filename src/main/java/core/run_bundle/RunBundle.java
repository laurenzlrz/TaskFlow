package core.run_bundle;

import core.nodes.Edge;
import core.nodes.IPipelineNode;
import core.graph_representation.IRepresentation;
import def_elements.DebugLog;

/**
 * Represents a run bundle in a pipeline node.
 *
 * @param <N> the type of the pipeline node.
 * @param <R> the type of the representation.
 */
class RunBundle<N extends IPipelineNode, R extends IRepresentation<N>> implements IRunBundle<N> {

    public static final boolean SLEEP_ON_LOCK = true;
    protected boolean abort;
    protected final R representation;

    /**
     * Constructs a RunBundle with the specified representation.
     *
     * @param representation the representation of the pipeline node.
     */
    public RunBundle(R representation) {
        this.representation = representation;
    }

    /**
     * Proceeds with the execution of the run bundle.
     */
    public void proceed() {

        if (this.abort) {
            this.after_abortion();
            return;
        }

        if (this.representation.areAllNodesFinished()) {
            this.afterFinish();
        }

        if (!this.representation.areNodesRunning() && !this.representation.areNodesFree()) {
            return;
        }

        N node = this.representation.takeNextNode(this);
        this.proceedOneThreadOneNode(node);
    }

    /**
     * Aborts the execution of the run bundle.
     */
    public void abort() {
        this.abort = true;
    }

    /**
     * Proceeds with the execution of one thread for one node.
     *
     * @param node the pipeline node to proceed with.
     */
    protected void proceedOneThreadOneNode(N node) {
        this.processOneNode(node);
        DebugLog.logThread("processed: " + node);
        this.representation.notifyFinished(node);
        DebugLog.logThread("proceedOneThreadOneNode finished: " + node);
    }

    /**
     * Processes one node in the run bundle.
     *
     * @param node the pipeline node to process.
     */
    protected void processOneNode(N node) {
        this.fabric_before_sign_in(node);
        DebugLog.logThread("Before sign in: " + node);
        node.SignInRunBundle(this, SLEEP_ON_LOCK);

        this.put(node);
        node.process();
        DebugLog.logThread("Before taking buffer: " + node);
        this.take(node);

        DebugLog.logThread("After immediate process: " + node);
        if (node.isAbortCondition()) {
            this.abort();
        }

        node.SignOutRunBundle(this);
        DebugLog.logThread("After sign out: " + node);
        this.fabric_after_sign_out(node);
    }

    /**
     * Puts the node's outgoing edges.
     *
     * @param node the pipeline node.
     */
    //TODO Clarify behaviour if next node is not free
    protected void put(N node) {
        for (Edge<?, N> edge : representation.getIncomingEdges(node)) {
            edge.put();
        }
    }

    /**
     * Takes the node's incoming edges.
     *
     * @param node the pipeline node.
     */
    protected void take(N node) {
        for (Edge<?, N> edge : representation.getOutgoingEdges(node)) {
            edge.take();
        }
    }

    /**
     * Hook method called before signing in a node.
     *
     * @param pipelineLoggingNode the pipeline node to sign in.
     */
    protected void fabric_before_sign_in(N pipelineLoggingNode) {
        this.before_sign_in(pipelineLoggingNode);
    }

    /**
     * Hook method for subclasses to perform actions before signing in.
     * This method is called before the `sign_in` method.
     * Subclasses can override this method to add custom behavior.
     *
     * @param pipelineLoggingNode the pipeline node to sign in.
     */
    protected void before_sign_in(N pipelineLoggingNode) {
    }

    /**
     * Hook method called after signing out a node.
     *
     * @param pipelineLoggingNode the pipeline node to sign out.
     */
    protected void fabric_after_sign_out(N pipelineLoggingNode) {
        this.after_sign_out(pipelineLoggingNode);
    }

    /**
     * Hook method for subclasses to perform actions after signing out.
     * For example, abort condition can be checked here.
     * This method is called after the `sign_out` method.
     * Subclasses can override this method to add custom behavior.
     *
     * @param pipelineLoggingNode the pipeline node to sign out.
     */
    protected void after_sign_out(N pipelineLoggingNode) {
    }

    /**
     * Hook method for subclasses to perform actions before abortion.
     * This method is called before the `abort` method.
     * Subclasses can override this method to add custom behavior.
     */
    protected void after_abortion() {
    }

    /**
     * Hook method for subclasses to perform actions after finishing.
     * This method is called after the `proceedOneThreadOneNode` method.
     * Subclasses can override this method to add custom behavior.
     */
    protected void afterFinish() {
    }

    /**
     * Hook method for subclasses to perform actions after one proceeding.
     * Subclasses can override this method to add custom behavior.
     */
    protected void afterOneProceeding() {
    }

}