import core.graph_representation.IRepresentation;
import core.graph_representation.SimpleRepresentation;
import core.logger.BundleLogger;
import core.logger.IBundleLogger;
import core.multithreading.IWorkerPool;
import core.multithreading.WorkerPool;
import core.nodes.BiFunctionPipelineNode;
import core.nodes.CallablePipelineNode;
import core.nodes.PipelineNode;
import core.run_bundle.IRunBundle;
import core.run_bundle.ParallelRunBundle;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.Callable;
import java.util.function.BiFunction;

public class SimplePipelineTest1 {

    Callable<Integer> callable1;
    Callable<Integer> callable2;
    BiFunction<Integer, Integer, Integer> biFunction1;

    CallablePipelineNode<Integer> callableNode1;
    CallablePipelineNode<Integer> callableNode2;
    BiFunctionPipelineNode<Integer, Integer, Integer> biFunctionNode1;

    SimpleRepresentation representation;
    IBundleLogger<PipelineNode> bundleLogger;
    IWorkerPool workerPool;
    IRunBundle<PipelineNode> runBundle;

    class Adder {
        public int add(int a, int b) {
            return a + b;
        }
    }

    @BeforeEach
    void setUp() {
        this.representation = new SimpleRepresentation();
        this.bundleLogger = new BundleLogger<>();
        this.workerPool = new WorkerPool(4);
        this.runBundle = new ParallelRunBundle<>(this.representation, this.bundleLogger, this.workerPool);

        this.callable1 = () -> 1;
        this.callable2 = () -> 2;

        Adder adder = new Adder();
        this.biFunction1 = (a, b) -> adder.add(a, b);

        this.callableNode1 = new CallablePipelineNode<>(this.callable1);
        this.callableNode2 = new CallablePipelineNode<>(this.callable2);
        this.biFunctionNode1 = new BiFunctionPipelineNode<>(this.biFunction1);

        this.representation.addCallableNode(this.callableNode1);
        this.representation.addCallableNode(this.callableNode2);
        this.representation.addBiFunctionNode(this.biFunctionNode1, this.callableNode1, this.callableNode2);
        this.representation.stipulateAll();
    }

    @Test
    void testRun() {
        new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                // Get all threads and print their state
                Thread.getAllStackTraces().keySet().forEach(thread -> {
                    System.out.println(thread.getName() + ": " + thread.getState());
                });
            }
        }).start();


        this.runBundle.proceed();
        int result = this.biFunctionNode1.getOutput().getBuffer();
        System.out.println(result);
    }
}
