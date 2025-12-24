package core.nodes;

import java.util.concurrent.Callable;

public class CallablePipelineNode<O> extends AbstractOutputNode<O> {

    protected Callable<O> callable;

    public CallablePipelineNode(Callable<O> callable) {
        this.callable = callable;
    }

    public void process() {
        try {
            O result = this.callable.call();
            this.set(this.output, result);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}