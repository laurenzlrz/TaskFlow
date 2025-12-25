package core.nodes;

public abstract class AbstractOutputNode<O> extends PipelineNode {

    protected Port<O, PipelineNode> output;

    public AbstractOutputNode() {
        this.output = new Port<>();
        this.setOutput(this.output, this);
    }

    public Port<O, PipelineNode> getOutput() {
        return this.output;
    }
}
