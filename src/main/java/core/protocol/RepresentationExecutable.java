package core.protocol;

import core.graph_representation.IRepresentation;
import core.nodes.IPipelineNode;
import core.run_bundle.IRunBundle;

/**
 * Implementation of Executable that bridges IRepresentation to the protocol interface.
 * This adapter allows the refactored WorkerPool to work with the existing DAG representation.
 * 
 * @param <N> the type of pipeline node
 */
public class RepresentationExecutable<N extends IPipelineNode> implements Executable<N> {
    
    private final IRepresentation<N> representation;
    private final IRunBundle<N> runBundle;
    
    /**
     * Creates a new RepresentationExecutable wrapping the given representation.
     * 
     * @param representation the representation to wrap
     * @param runBundle the run bundle for context
     */
    public RepresentationExecutable(IRepresentation<N> representation, IRunBundle<N> runBundle) {
        this.representation = representation;
        this.runBundle = runBundle;
    }
    
    /**
     * Retrieves the next node that is ready for execution.
     * Returns null if no node is currently available.
     * 
     * @return the next node to execute, or null if none available
     */
    @Override
    public N checkIn() {
        if (!representation.areNodesAvailable()) {
            return null;
        }
        return representation.takeNextNode(runBundle);
    }
    
    /**
     * Reports the completion of a node execution.
     * 
     * @param node the node that has been completed
     */
    @Override
    public void checkOut(N node) {
        representation.notifyFinished(node);
    }
    
    /**
     * Returns the number of nodes that are currently ready for execution.
     * 
     * @return the number of available nodes
     */
    @Override
    public int getAvailableTaskCount() {
        if (!representation.areNodesAvailable()) {
            return 0;
        }
        // We can't easily count without side effects, so return 1 if available
        // This is a conservative estimate that ensures workers wake up when there's work
        return 1;
    }
    
    /**
     * Checks if all nodes have been completed.
     * 
     * @return true if all nodes are finished
     */
    public boolean areAllNodesFinished() {
        return representation.areAllNodesFinished();
    }
    
    /**
     * Checks if there are nodes currently running.
     * 
     * @return true if nodes are running
     */
    public boolean areNodesRunning() {
        return representation.areNodesRunning();
    }
    
    /**
     * Checks if there are free nodes that could run.
     * 
     * @return true if nodes are free
     */
    public boolean areNodesFree() {
        return representation.areNodesFree();
    }
}
