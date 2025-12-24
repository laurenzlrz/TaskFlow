package core.logger;

import core.graph_representation.IRepresentation;
import core.nodes.Edge;
import core.nodes.IPipelineNode;
import core.run_bundle.LoggingRunBundle;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.ThreadMXBean;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Class responsible for logging the state and resource usage of pipeline nodes.
 *
 * @param <N> the type of pipeline node
 */
public class BundleLogger<N extends IPipelineNode> implements IBundleLogger<N> {

    public static final String GRAPH_FORMAT_STRING = "digraph G {\n";
    public static final String NODE_FORMAT_STRING = "  \"%s\" -> \"%s\";\n";
    protected final MemoryMXBean memoryMXBean;
    protected final ThreadMXBean threadMXBean;
    protected long lastTimestamp;
    protected long currentTimestamp;
    public Map<LoggingRunBundle<N,?,?>, Map<N, LoggingRecord<N>>> records;
    protected Map<LoggingRunBundle<N,?,?>, GraphRecord<N>> graphs;
    public static final String DEFAULT_PHASE = "DEFAULT";

    /**
     * Constructs a new BundleLogger.
     */
    public BundleLogger() {
        this.memoryMXBean = ManagementFactory.getMemoryMXBean();
        this.threadMXBean = ManagementFactory.getThreadMXBean();
        this.records = new ConcurrentHashMap<>();
    }

    /**
     * Begins logging for the given node and bundle.
     *
     * @param node the pipeline node
     * @param bundle the logging run bundle
     */
    public void begin(N node, LoggingRunBundle<N,?,?> bundle) {
        this.lastTimestamp = Instant.now().toEpochMilli();
    }

    /**
     * Ends logging for the given node and bundle.
     *
     * @param node the pipeline node
     * @param bundle the logging run bundle
     */
    public void end(N node, LoggingRunBundle<N,?,?> bundle) {
        this.currentTimestamp = Instant.now().toEpochMilli();
        long deltaTime = this.currentTimestamp - this.lastTimestamp;
        this.logResourceUsage(node, bundle, deltaTime);
    }

    /**
     * Logs the resource usage for the given node and bundle.
     *
     * @param node the pipeline node
     * @param bundle the logging run bundle
     * @param deltaTime the time difference since the last log
     */
    protected void logResourceUsage(N node, LoggingRunBundle<N,?,?> bundle, Long deltaTime) {

        synchronized (this) {
            if (!this.records.containsKey(bundle)) {
                this.records.put(bundle, new ConcurrentHashMap<>());
            }
        }

        long timestamp = Instant.now().toEpochMilli();
        long usedHeapMemory = memoryMXBean.getHeapMemoryUsage().getUsed();
        long usedNonHeapMemory = memoryMXBean.getNonHeapMemoryUsage().getUsed();
        int activeThreads = Thread.activeCount();
        long currentThreadCpuTime = threadMXBean.getCurrentThreadCpuTime();
        long freeMemory = Runtime.getRuntime().freeMemory();
        long totalMemory = Runtime.getRuntime().totalMemory();
        long maxMemory = Runtime.getRuntime().maxMemory();
        String threadName = Thread.currentThread().getName();
        int availableProcessors = Runtime.getRuntime().availableProcessors();

        LoggingRecord<N> record = new LoggingRecord<>(
                DEFAULT_PHASE, node, bundle, timestamp, deltaTime, usedHeapMemory, usedNonHeapMemory, freeMemory,
                totalMemory, maxMemory, activeThreads, currentThreadCpuTime, threadName, availableProcessors
        );

        this.records.get(bundle).put(node, record);
    }

    /**
     * Logs the graph representation for the given bundle.
     *
     * @param representation the graph representation
     * @param bundle the logging run bundle
     */
    protected void logGraph(IRepresentation<N> representation, LoggingRunBundle<N,?,?> bundle) {
        GraphRecord<N> graphRecord = representation.getGraph();
        this.graphs.put(bundle, graphRecord);
    }

    /**
     * Prints the graph representation for the given bundle in DOT format.
     *
     * @param bundle the logging run bundle
     * @return the graph representation in DOT format
     */
    protected String printGraph(LoggingRunBundle<N,?,?> bundle) {
        GraphRecord<N> graphRecord = this.graphs.get(bundle);

        StringBuilder dot = new StringBuilder();
        dot.append(GRAPH_FORMAT_STRING);

        for (Map.Entry<N, List<Edge<?, N>>> entry : graphRecord.outgoingEdges().entrySet()) {
            N sourceNode = entry.getKey();
            for (Edge<?, N> edge : entry.getValue()) {
                dot.append(String.format(NODE_FORMAT_STRING,
                        edge.from.getNode().toDescription(),
                        edge.to.getNode().toDescription()));
            }
        }

        dot.append("}");
        return dot.toString();
    }
}