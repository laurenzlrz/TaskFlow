package core.nodes;

import core.run_bundle.IRunBundle;

import java.util.List;

/**
 * Interface representing a pipeline node.
 */
public interface IPipelineNode {

    /**
     * Gets the input ports of the pipeline node.
     *
     * @return a list of input ports
     */
    List<IPort<?, ?>> getInputs();

    /**
     * Gets the output ports of the pipeline node.
     *
     * @return a list of output ports
     */
    List<IPort<?, ?>> getOutputs();

    /**
     * Signs in the pipeline node to a run bundle.
     *
     * @param bundle the run bundle
     * @param sleep whether to sleep during the sign-in process
     * @return true if the sign-in was successful, false otherwise
     */
    boolean SignInRunBundle(IRunBundle<?> bundle, boolean sleep);

    /**
     * Signs out the pipeline node from a run bundle.
     *
     * @param bundle the run bundle
     */
    void SignOutRunBundle(IRunBundle<?> bundle);

    /**
     * Provides a description of the pipeline node.
     *
     * @return a description of the pipeline node
     */
    String toDescription();

    /**
     * Processes the pipeline node.
     */
    void process();

    /**
     * Checks if the pipeline node is in an abort condition.
     *
     * @return true if the node is in an abort condition, false otherwise
     */
    boolean isAbortCondition();

    /**
     * Sets the input ports of the pipeline node.
     *
     * @param inputs a list of input ports
     */
    <T> void setInputs(List<IPort<?, T>> inputs, T node);

    /**
     * Sets the output ports of the pipeline node.
     *
     * @param outputs a list of output ports
     */
    <T> void setOutputs(List<IPort<?, T>> outputs, T node);

    <T> void setInput(IPort<?, T> input, T node);

    <T> void setOutput(IPort<?, T> output, T node);

    /**
     * Notifies the pipeline node that a port has been added.
     *
     * @param port the port that was added
     */
    void notifyAdded(IPort<?, ?> port);

    /**
     * Notifies the pipeline node that a port has been removed.
     *
     * @param port the port that was removed
     */
    void notifyRemoved(IPort<?, ?> port);
}