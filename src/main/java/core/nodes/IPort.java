package core.nodes;

/**
 * Interface representing a port in a pipeline node.
 *
 * @param <T> the type of data being transferred through the port
 * @param <N> the type of pipeline node
 */
public interface IPort<T, N> {

    /**
     * Gets the data from the buffer and resets the buffer.
     *
     * @return the data from the buffer
     */
    T getAndResetBuffer();

    /**
     * Sets the data in the buffer and notifies the relevant components.
     *
     * @param buffer the data to be set in the buffer
     */
    void setBufferAndNotify(T buffer);

    /**
     * Gets the pipeline node associated with this port.
     *
     * @return the pipeline node
     */
    N getNode();

    /**
     * Checks if the port is currently in use.
     *
     * @return true if the port is in use, false otherwise
     */
    boolean isUsed();

    void setNode(N node);
}