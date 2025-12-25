package core.graph_representation;

import core.nodes.*;

public class SimpleRepresentation extends ParallelRepresentation<PipelineNode> {

    public static final String INPUT_NODE_NOT_IN_GRAPH_MSG = "Input node not in graph";

    public <I1, I2> void addBiFunctionNode(BiFunctionPipelineNode<I1,I2,?> node,
                                           AbstractOutputNode<I1> input1, AbstractOutputNode<I2> input2) {
        this.addNode(node);
        this.checkIfInputNodeExists(input1);
        this.checkIfInputNodeExists(input2);

        this.addEdge(input1.getOutput(), node.getInput1());
        this.addEdge(input2.getOutput(), node.getInput2());
    }

    public <I> void addFunctionNode(FunctionPipelineNode<I,?> node, AbstractOutputNode<I> input) {
        this.addNode(node);
        this.checkIfInputNodeExists(input);

        this.addEdge(input.getOutput(), node.getInput());
    }

    public <I1, I2> void addBiConsumerNode(BiConsumerPipelineNode<I1,I2> node,
                                           AbstractOutputNode<I1> input1, AbstractOutputNode<I2> input2) {
        this.addNode(node);
        this.checkIfInputNodeExists(input1);
        this.checkIfInputNodeExists(input2);

        this.addEdge(input1.getOutput(), node.getInput1());
        this.addEdge(input2.getOutput(), node.getInput2());
    }

    public <I> void addConsumerNode(ConsumerPipelineNode<I> node, AbstractOutputNode<I> input) {
        this.addNode(node);

        this.checkIfInputNodeExists(input);

        this.addEdge(input.getOutput(), node.getInput());
    }

    public <O> void addCallableNode(CallablePipelineNode<O> node) {
        this.addNode(node);
    }

    private void checkIfInputNodeExists(AbstractOutputNode<?> node) {
        if (!this.nodeGraph.containsKey(node)) {
            throw new IllegalArgumentException(INPUT_NODE_NOT_IN_GRAPH_MSG);
        }
    }
}
