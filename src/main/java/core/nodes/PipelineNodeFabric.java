package core.nodes;

/**
 * Abstract class representing a fabric for creating pipeline nodes.
 */
abstract class PipelineNodeFabric {

    /**
     * Hook method called when getting a value from a port.
     *
     * @param <T> the type of the value.
     * @param port the port from which the value is being retrieved.
     */
    protected <T> void at_getting(IPort<T, ?> port) {
    }

    /**
     * Hook method called when setting a value to a port.
     *
     * @param <T> the type of the value.
     * @param port the port to which the value is being set.
     */
    protected <T> void at_setting(IPort<T, ?> port) {
    }

    /**
     * Notifies that a port has been added.
     *
     * @param port the port that has been added.
     */
    public void notifyAdded(IPort<?, ?> port) {
    }

    /**
     * Notifies that a port has been removed.
     *
     * @param port the port that has been removed.
     */
    public void notifyRemoved(IPort<?, ?> port) {
    }
}