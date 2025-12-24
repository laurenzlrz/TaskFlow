package core.nodes;

import java.util.function.Function;

public class FunctionPipelineNode<I,O> extends AbstractOutputNode<O> {

    protected Function<I,O> function;
    protected Port<I, PipelineNode> input;
    protected Port<O, PipelineNode> output;

    public FunctionPipelineNode(Function<I,O> function) {
        this.function = function;
        this.input = new Port<>();
        this.output = new Port<>();
        this.setInput(this.input, this);
        this.setOutput(this.output, this);
    }

    public void process() {
        I input = this.get(this.input);
        O output = this.function.apply(input);
        this.set(this.output, output);
    }

    public Port<I, PipelineNode> getInput() {
        return this.input;
    }
}
