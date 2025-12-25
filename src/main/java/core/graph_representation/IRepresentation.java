package core.graph_representation;

import core.logger.GraphRecord;
import core.nodes.Edge;
import core.nodes.IPipelineNode;
import core.nodes.IPort;
import core.run_bundle.IRunBundle;

import java.util.List;

/**
 * Interface representing a graph structure for pipeline nodes.
 *
 * @param <N> the type of pipeline node
 */
public interface IRepresentation<N extends IPipelineNode> {

    /**
     * Notifies that the given pipeline node has finished processing.
     *
     * @param pipelineNode the pipeline node that has finished
     */
    void notifyFinished(N pipelineNode);

    /**
     * Takes the next node to be processed from the run bundle.
     *
     * @param runBundle the run bundle containing nodes
     * @return the next pipeline node to be processed
     */
    N takeNextNode(IRunBundle<N> runBundle);

    /**
     * Stipulates the given node in the graph.
     *
     * @param node the node to be stipulated
     */
    void stipulateNode(N node);

    /**
     * Stipulates all nodes in the graph.
     */
    void stipulateAll();

    /**
     * Adds a node to the graph.
     *
     * @param node the node to be added
     */
    void addNode(N node);

    /**
     * Adds an edge between two ports in the graph.
     *
     * @param from the source port
     * @param to the destination port
     * @param <T> the type of data transferred through the ports
     */
    <T> void addEdge(IPort<T, N> from, IPort<T, N> to);

    /**
     * Gets the outgoing edges from the given node.
     *
     * @param node the node whose outgoing edges are to be retrieved
     * @return a list of outgoing edges from the node
     */
    List<Edge<?, N>> getOutgoingEdges(N node);

    /**
     * Gets the incoming edges to the given node.
     *
     * @param node the node whose incoming edges are to be retrieved
     * @return a list of incoming edges to the node
     */
    List<Edge<?, N>> getIncomingEdges(N node);

    /**
     * Checks if all nodes in the graph have finished processing.
     *
     * @return true if all nodes are finished, false otherwise
     */
    boolean areAllNodesFinished();

    /**
     * Checks if there are any nodes available for processing.
     *
     * @return true if nodes are available, false otherwise
     */
    boolean areNodesAvailable();

    /**
     * Gets the graph record representing the current state of the graph.
     *
     * @return the graph record
     */
    GraphRecord<N> getGraph();

    boolean areNodesRunning();

    boolean areNodesFree();
}
