package core.nodes;

import core.nodes.IPipelineNode;
import core.nodes.IPort;
import def_elements.DebugLog;

/**
 * Represents a port in a pipeline node.
 *
 * @param <T> the type of the value held by the port.
 * @param <N> the type of the pipeline node.
 */
public class Port<T, N extends IPipelineNode> implements IPort<T, N> {
    public static final String BUFFER_ALREADY_SET_MSG = "Buffer already set";
    public static final String BUFFER_NOT_SET_MSG = "Buffer not set";

    private T buffer;
    private N node;
    private boolean isUsed;


    /**
     * Gets the buffer value of the port.
     *
     * @return the buffer value.
     */
    public T getBuffer() {
        return this.buffer;
    }

    /**
     * Gets the buffer value and resets it.
     *
     * @return the buffer value.
     * @throws IllegalStateException if the buffer is not set.
     */
    public T getAndResetBuffer() {
        if (!this.isUsed) {
            throw new IllegalStateException(BUFFER_NOT_SET_MSG);
        }
        T buffer = this.buffer;
        this.buffer = null;
        this.node.notifyRemoved(this);
        this.isUsed = false;
        return buffer;
    }

    /**
     * Sets the buffer value of the port.
     *
     * @param buffer the buffer value to set.
     */
    public void setBuffer(T buffer) {
        this.buffer = buffer;
        this.isUsed = true;
    }

    /**
     * Sets the buffer value and notifies the node.
     *
     * @param buffer the buffer value to set.
     * @throws IllegalStateException if the buffer is already set.
     */
    public void setBufferAndNotify(T buffer) {
        if (this.isUsed) {
            throw new IllegalStateException(BUFFER_ALREADY_SET_MSG);
        }
        this.isUsed = true;
        this.buffer = buffer;
        DebugLog.logThread("Port setBufferAndNotify: " + this + " with buffer: " + buffer);
        this.node.notifyAdded(this);
    }

    /**
     * Gets the pipeline node to which this port belongs.
     *
     * @return the pipeline node.
     */
    public N getNode() {
        return this.node;
    }

    /**
     * Checks if the port is used.
     *
     * @return true if the buffer is set, false otherwise.
     */
    public boolean isUsed() {
        return this.buffer != null;
    }

    /**
     * Sets the pipeline node to which this port belongs.
     *
     * @param node the pipeline node.
     */
    public void setNode(N node) {
        this.node = node;
    }
}