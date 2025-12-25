package core.nodes;

import java.util.function.BiConsumer;

public class BiConsumerPipelineNode<I1, I2> extends PipelineNode {

    protected BiConsumer<I1, I2> biConsumer;
    protected Port<I1, PipelineNode> input1;
    protected Port<I2, PipelineNode> input2;

    public BiConsumerPipelineNode(BiConsumer<I1, I2> biConsumer) {
        this.biConsumer = biConsumer;
        this.input1 = new Port<>();
        this.input2 = new Port<>();
        this.setInput(this.input1, this);
        this.setInput(this.input2, this);
    }

    public void process() {
        I1 input1 = this.get(this.input1);
        I2 input2 = this.get(this.input2);
        this.biConsumer.accept(input1, input2);
    }

    public Port<I1, PipelineNode> getInput1() {
        return this.input1;
    }

    public Port<I2, PipelineNode> getInput2() {
        return this.input2;
    }
}