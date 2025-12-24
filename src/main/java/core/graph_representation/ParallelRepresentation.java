package core.graph_representation;

import core.logger.GraphRecord;
import core.nodes.Edge;
import core.nodes.IPipelineNode;
import core.nodes.IPort;
import core.run_bundle.IRunBundle;
import def_elements.DebugLog;

import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.Queue;
import java.util.List;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * Class representing a parallel graph structure for pipeline nodes.
 *
 * @param <N> the type of pipeline node
 */
public class ParallelRepresentation<N extends IPipelineNode> implements IRepresentation<N> {

    public static final String NODE_ALREADY_EXISTS_MSG = "Node already exists";
    public static final String NODE_NOT_FOUND_MSG = "Node not found";
    public static final String PORT_ALREADY_USED = "Port already existing";
    public static final String NODE_NOT_MUTABLE_ANYMORE_MSG = "Node not mutable anymore";
    public static final String PORTS_NOT_CONNECTED_MSG = "not all Ports connected";
    public static final String NULL_MSG = "input is null";

    protected Set<Edge<?, N>> edges;
    protected Map<IPort<?, N>, Integer> portInboundIndex;
    protected Map<N, List<Edge<?, N>>> incomingEdges;
    protected Map<N, List<Edge<?, N>>> outgoingEdges;
    protected Map<N, List<N>> nodeGraph;
    protected Map<N, AtomicInteger> inDegree;

    protected Set<N> setted;
    protected Queue<N> freeSteps;
    protected List<N> currentlyRunning;
    protected List<N> finished;
    protected ReentrantLock lock;

    /**
     * Constructs a new ParallelRepresentation.
     */
    public ParallelRepresentation() {
        this.edges = new HashSet<>();
        this.portInboundIndex = new ConcurrentHashMap<>();
        this.incomingEdges = new ConcurrentHashMap<>();
        this.outgoingEdges = new ConcurrentHashMap<>();
        this.nodeGraph = new ConcurrentHashMap<>();
        this.inDegree = new ConcurrentHashMap<>();

        this.setted = new HashSet<>();

        this.freeSteps = new ConcurrentLinkedDeque<>();
        this.currentlyRunning = Collections.synchronizedList(new LinkedList<>());
        this.finished = Collections.synchronizedList(new ArrayList<>());

        this.lock = new ReentrantLock();
    }

    @Override
    public GraphRecord<N> getGraph() {
        return new GraphRecord<>(Map.copyOf(this.incomingEdges), Map.copyOf(this.outgoingEdges));
    }

    @Override
    public boolean areNodesRunning() {
        return !this.currentlyRunning.isEmpty();
    }

    @Override
    public boolean areNodesFree() {
        return !this.freeSteps.isEmpty();
    }

    @Override
    public boolean areAllNodesFinished() {
        return this.finished.size() == this.nodeGraph.size();
    }

    @Override
    public boolean areNodesAvailable() {
        return !this.freeSteps.isEmpty();
    }

    @Override
    public void notifyFinished(N node) {
        DebugLog.logThread("gonna check currentlyRunning: " + node);
        if (node == null) {
            throw new IllegalArgumentException(NULL_MSG);
        }

        if (!this.currentlyRunning.contains(node)) {
            throw new IllegalArgumentException("Node not running");
        }

        assertConsistency1(node);

        DebugLog.logThread("gonna remove from currentlyRunning: " + node);
        this.currentlyRunning.remove(node);
        DebugLog.logThread("removed" + node);
        List<N> successors = this.getSuccesors(node);
        DebugLog.logThread("gonna decrement successors: " + successors);
        successors.forEach(this::decrementSuccessorDegree);
        this.finished.add(node);
        DebugLog.logThread("added to finished: " + node);
    }

    @Override
    public void addNode(N node) {
        if (node == null) {
            throw new IllegalArgumentException(NULL_MSG);
        }

        if (this.nodeGraph.containsKey(node) || this.inDegree.containsKey(node)) {
            throw new IllegalArgumentException(NODE_ALREADY_EXISTS_MSG);
        }

        assert !this.currentlyRunning.contains(node);
        assert !this.finished.contains(node);
        assert !this.freeSteps.contains(node);
        assert !this.nodeGraph.containsKey(node);
        assert !this.incomingEdges.containsKey(node);
        assert !this.outgoingEdges.containsKey(node);
        assert !this.inDegree.containsKey(node);
        assert !this.setted.contains(node);

        this.nodeGraph.put(node, new ArrayList<>());
        this.inDegree.put(node, new AtomicInteger(0));
        this.outgoingEdges.put(node, new ArrayList<>());
        this.incomingEdges.put(node, new ArrayList<>());
    }

    @Override
    public <T> void addEdge(IPort<T, N> from, IPort<T, N> to) {
        if (from == null || to == null) {
            throw new IllegalArgumentException(NULL_MSG);
        }

        N toNode = to.getNode();
        N fromNode = from.getNode();

        if (!this.nodeGraph.containsKey(fromNode) || !this.nodeGraph.containsKey(toNode)) {
            throw new IllegalArgumentException(NODE_NOT_FOUND_MSG);
        }

        if (this.portInboundIndex.containsKey(to)) {
            throw new IllegalArgumentException(PORT_ALREADY_USED);
        }

        if (this.setted.contains(toNode) || this.setted.contains(fromNode)) {
            throw new IllegalArgumentException(NODE_NOT_MUTABLE_ANYMORE_MSG);
        }

        assert !this.portInboundIndex.containsKey(to);
        assertConsistency2(toNode);
        assertConsistency2(fromNode);

        Edge<T, N> edge = new Edge<>(from, to);
        this.outgoingEdges.get(fromNode).add(edge);
        this.incomingEdges.get(toNode).add(edge);
        this.edges.add(edge);

        if (!this.nodeGraph.get(toNode).contains(fromNode)) {
            this.nodeGraph.get(toNode).add(fromNode);
            this.inDegree.get(toNode).incrementAndGet();
        }
    }

    @Override
    public void stipulateNode(N node) {
        if (!this.nodeGraph.containsKey(node) || !this.inDegree.containsKey(node)) {
            throw new IllegalArgumentException(NODE_NOT_FOUND_MSG);
        }

        if (this.setted.contains(node)) {
            throw new IllegalArgumentException(NODE_NOT_MUTABLE_ANYMORE_MSG);
        }

        List<IPort<?,?>> inputs = node.getInputs();
        if (inputs != null) {
            HashSet<IPort<?, ?>> ports = new HashSet<>(node.getInputs());
            for (Edge<?, N> edge : this.incomingEdges.get(node)) {
                ports.remove(edge.to);
            }
            if (!ports.isEmpty()) {
                throw new IllegalArgumentException(PORTS_NOT_CONNECTED_MSG);
            }
        }

        assert !this.currentlyRunning.contains(node);
        assert !this.finished.contains(node);
        assert !this.setted.contains(node);
        assert !this.freeSteps.contains(node);
        assert this.nodeGraph.containsKey(node);
        assert this.inDegree.containsKey(node);

        this.setted.add(node);
        if (this.inDegree.get(node).get() == 0) {
            this.freeSteps.add(node);
        }
    }

    @Override
    public void stipulateAll() {
        this.nodeGraph.keySet().forEach(this::stipulateNode);
    }

    @Override
    public N takeNextNode(IRunBundle<N> runBundle) {
        N node = this.freeSteps.poll();
        if (node != null) {
            this.currentlyRunning.add(node);
            assertConsistency1(node);
        }
        return node;
    }

    @Override
    public List<Edge<?, N>> getOutgoingEdges(N node) {
        return this.outgoingEdges.get(node);
    }

    @Override
    public List<Edge<?, N>> getIncomingEdges(N node) {
        return this.incomingEdges.get(node);
    }

    /**
     * Gets the successors of the given node.
     *
     * @param node the node whose successors are to be retrieved
     * @return a list of successor nodes
     */
    protected List<N> getSuccesors(N node) {
        return this.getOutgoingEdges(node).stream().
                map(edge -> edge.to.getNode()).
                collect(Collectors.toList());
    }

    /**
     * Gets the predecessors of the given node.
     *
     * @param node the node whose predecessors are to be retrieved
     * @return a list of predecessor nodes
     */
    protected List<N> getPredecessors(N node) {
        return this.getIncomingEdges(node).stream().
                map(edge -> edge.from.getNode()).
                collect(Collectors.toList());
    }

    /**
     * Decrements the in-degree of the given successor node.
     *
     * @param successor the successor node
     */
    protected void decrementSuccessorDegree(N successor) {
        int result = this.inDegree.get(successor).decrementAndGet();
        if (result == 0 && this.setted.contains(successor)) {
            this.freeSteps.add(successor);
        }
    }

    /**
     * Asserts the consistency of the given node.
     *
     * @param node the node to be checked
     */
    private void assertConsistency1(N node) {
        assert this.currentlyRunning.contains(node);
        assert !this.finished.contains(node);
        assert !this.freeSteps.contains(node);
        assert this.nodeGraph.containsKey(node);
        assert this.incomingEdges.containsKey(node);
        assert this.outgoingEdges.containsKey(node);
        assert this.inDegree.containsKey(node);
        assert this.setted.contains(node);
    }

    /**
     * Asserts the consistency of the given node.
     *
     * @param toNode the node to be checked
     */
    protected void assertConsistency2(N toNode) {
        assert !this.currentlyRunning.contains(toNode);
        assert !this.finished.contains(toNode);
        assert !this.freeSteps.contains(toNode);
        assert !this.setted.contains(toNode);
        assert this.nodeGraph.containsKey(toNode);
        assert this.incomingEdges.containsKey(toNode);
        assert this.outgoingEdges.containsKey(toNode);
        assert this.inDegree.containsKey(toNode);
    }
}