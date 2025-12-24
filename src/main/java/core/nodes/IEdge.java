package core.nodes;

/**
 * Interface representing an edge in a pipeline, connecting two ports.
 *
 * @param <T> the type of data being transferred through the edge
 * @param <N> the type of pipeline node
 */
public interface IEdge<T, N extends IPipelineNode> {

    /**
     * Takes the data from the source port and stores it in the buffer.
     */
    void take();

    /**
     * Puts the data from the buffer into the destination port.
     */
    void put();
}