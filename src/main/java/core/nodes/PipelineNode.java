package core.nodes;

import core.run_bundle.IRunBundle;
import def_elements.DebugLog;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Represents a node in a pipeline that processes data.
 */
public class PipelineNode extends PipelineNodeFabric implements IPipelineNode {
    public static final String PORT_NOT_FOUND_MSG = "Port not found";
    public static final String NOT_SIGNED_IN_MSG = "Not signed in";
    public static final String PORT_NOT_BELONGING_TO_NODE_MSG = "Input port does not belong to this node";

    protected List<IPort<?, ?>> inputs;
    protected List<IPort<?, ?>> outputs;
    protected final ReentrantLock lock;
    protected IRunBundle<?> currentBundle;

    /**
     * Constructs a PipelineNode with a new ReentrantLock.
     */
    public PipelineNode() {
        this.lock = new ReentrantLock();
        this.inputs = Collections.emptyList();
        this.outputs = Collections.emptyList();
    }

    /**
     * Processes the data in the pipeline node.
     */
    public void process() {
        for (IPort<?,?> output : this.outputs) {
            output.setBufferAndNotify(null);
        }
    }

    /**
     * Checks if the abort condition is met.
     *
     * @return true if the abort condition is met, false otherwise.
     */
    public boolean isAbortCondition() {
        return false;
    }

    /**
     * Sets the input ports for the pipeline node.
     *
     * @param inputs the list of input ports.
     */
    public <T> void setInputs(List<IPort<?, T>> inputs, T node) {
        this.inputs = new ArrayList<>();
        for (IPort<?, T> input : inputs) {
            if (input.getNode() != null) {
                throw new IllegalArgumentException(PORT_NOT_BELONGING_TO_NODE_MSG);
            }
            input.setNode(node);
            this.inputs.add(input);

        }
    }

    /**
     * Sets the output ports for the pipeline node.
     *
     * @param outputs the list of output ports.
     */
    public <T> void setOutputs(List<IPort<?, T>> outputs, T node) {
        this.outputs = new ArrayList<>();
        for (IPort<?, T> output : outputs) {
            if (output.getNode() != null) {
                throw new IllegalArgumentException(PORT_NOT_BELONGING_TO_NODE_MSG);
            }
            output.setNode(node);
            this.outputs.add(output);
        }
    }

    public <T> void setInput(IPort<?, T> input, T node) {
        List<IPort<?,T>> inputs = new ArrayList<>();
        inputs.add(input);
        this.setInputs(inputs, node);
    }

    public <T> void setOutput(IPort<?, T> output, T node) {
        List<IPort<?,T>> outputs = new ArrayList<>();
        outputs.add(output);
        this.setOutputs(outputs, node);
    }

    /**
     * Gets the value from the specified input port.
     *
     * @param <T> the type of the value.
     * @param inputPort the input port.
     * @return the value from the input port.
     * @throws IllegalArgumentException if the input port is not found.
     */
    protected <T> T get(Port<T, ?> inputPort) {
        if (!this.inputs.contains(inputPort)) {
            throw new IllegalArgumentException(PORT_NOT_FOUND_MSG);
        }
        this.at_getting(inputPort);
        return inputPort.getBuffer();
    }

    /**
     * Sets the value to the specified output port.
     *
     * @param <T> the type of the value.
     * @param outputPort the output port.
     * @param value the value to set.
     * @throws IllegalArgumentException if the output port is not found.
     */
    protected <T> void set(Port<T, ?> outputPort, T value) {
        if (!this.outputs.contains(outputPort)) {
            throw new IllegalArgumentException(PORT_NOT_FOUND_MSG);
        }
        this.at_setting(outputPort);
        outputPort.setBuffer(value);
    }

    /**
     * Gets the list of input ports.
     *
     * @return an unmodifiable list of input ports.
     */
    public List<IPort<?, ?>> getInputs() {
        return List.copyOf(this.inputs);
    }

    /**
     * Gets the list of output ports.
     *
     * @return an unmodifiable list of output ports.
     */
    public List<IPort<?, ?>> getOutputs() {
        return List.copyOf(this.outputs);
    }

    /**
     * Signs in a run bundle to the pipeline node.
     *
     * @param bundle the run bundle to sign in.
     * @param sleep whether to lock the node.
     * @return true if the bundle was signed in, false otherwise.
     */
    public boolean SignInRunBundle(IRunBundle<?> bundle, boolean sleep) {
        DebugLog.logThread("SignInRunBundle Function at: " + this);
        if (sleep) {
            this.lock.lock();
        }

        if (!sleep && !this.lock.tryLock()) {
            return false;
        }

        this.currentBundle = bundle;
        return true;
    }

    /**
     * Signs out a run bundle from the pipeline node.
     *
     * @param bundle the run bundle to sign out.
     * @throws IllegalStateException if the bundle is not signed in.
     */
    public void SignOutRunBundle(IRunBundle<?> bundle) {
        if (this.currentBundle != bundle) {
            throw new IllegalStateException(NOT_SIGNED_IN_MSG);
        }
        DebugLog.logThread("SignOutRunBundle Function at: " + this);
        this.lock.unlock();
    }

    /**
     * Provides a description of the pipeline node.
     *
     * @return a string description of the pipeline node.
     */
    public String toDescription() {
        String desc = this.getClass().getSimpleName();
        desc += "\n";
        desc += String.format("Inputs: %s,\nOutputs: %s", this.inputs.toString(), this.outputs.toString());
        return desc;
    }
}