package core.nodes;

import java.util.function.Consumer;

public class ConsumerPipelineNode<I> extends PipelineNode {
    protected Consumer<I> consumer;
    protected Port<I, PipelineNode> input;

    public ConsumerPipelineNode(Consumer<I> consumer) {
        this.consumer = consumer;
        this.input = new Port<>();
        this.setInput(this.input, this);
    }

    public void process() {
        I input = this.get(this.input);
        this.consumer.accept(input);
    }

    public Port<I, PipelineNode> getInput() {
        return this.input;
    }
}
