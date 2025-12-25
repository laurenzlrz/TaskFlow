package core.nodes;

import java.util.Objects;

/**
 * Class representing an edge in a pipeline, connecting two ports.
 *
 * @param <T> the type of data being transferred through the edge
 * @param <N> the type of pipeline node
 */
public class Edge<T, N extends IPipelineNode> implements IEdge<T, N> {
    /**
     * The source port of the edge.
     */
    public final IPort<T, N> from;

    /**
     * The destination port of the edge.
     */
    public final IPort<T, N> to;

    /**
     * The buffer holding the data being transferred.
     */
    public T buffer;

    /**
     * Constructs an Edge with the specified source and destination ports.
     *
     * @param from the source port
     * @param to the destination port
     */
    public Edge(IPort<T, N> from, IPort<T, N> to) {
        this.from = from;
        this.to = to;
    }

    /**
     * Takes the data from the source port and stores it in the buffer.
     */
    public void take() {
        this.buffer = this.from.getAndResetBuffer();
    }

    /**
     * Puts the data from the buffer into the destination port.
     */
    public void put() {
        this.to.setBufferAndNotify(this.buffer);
    }

    /**
     * Checks if this edge is equal to another object.
     *
     * @param obj the object to compare with
     * @return true if the edges are equal, false otherwise
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        Edge<?, ?> edge = (Edge<?, ?>) obj;
        return Objects.equals(from, edge.from) && Objects.equals(to, edge.to);
    }

    /**
     * Returns the hash code of this edge.
     *
     * @return the hash code of this edge
     */
    @Override
    public int hashCode() {
        return Objects.hash(from, to);
    }
}