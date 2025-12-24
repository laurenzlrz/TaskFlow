package core.nodes;

import def_elements.DebugLog;

import java.util.List;
import java.util.function.BiFunction;

public class BiFunctionPipelineNode<I1, I2, O> extends AbstractOutputNode<O> {

    protected BiFunction<I1, I2, O> function;
    protected Port<I1, PipelineNode> input1;
    protected Port<I2, PipelineNode> input2;


    public BiFunctionPipelineNode(BiFunction<I1, I2, O> function) {
        super();
        this.input1 = new Port<>();
        this.input2 = new Port<>();

        this.setInputs(List.of(input1, input2), this);

        this.function = function;
    }

    public void process() {
        I1 input1 = this.get(this.input1);
        I2 input2 = this.get(this.input2);
        DebugLog.logThread("BiFunctionPipelineNode.process input: " + input1 + ", " + input2);
        DebugLog.logThread("Funktion-Klasse: " + this.function.getClass().getName());
        O result = null;
        try {
            result = this.function.apply(input1, input2);
        } catch (Exception e) {
            this.notifyAll();
            throw new RuntimeException(e);
        }
        DebugLog.logThread("BiFunctionPipelineNode.process result: " + result);
        this.set(this.output, result);
    }

    public Port<I1, PipelineNode> getInput1() {
        return this.input1;
    }

    public Port<I2, PipelineNode> getInput2() {
        return this.input2;
    }
}
