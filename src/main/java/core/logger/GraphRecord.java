package core.logger;

import core.nodes.Edge;
import core.nodes.IPipelineNode;

import java.util.List;
import java.util.Map;

/**
 * Record representing the graph structure with incoming and outgoing edges for pipeline nodes.
 *
 * @param <N> the type of pipeline node
 */
public record GraphRecord<N extends IPipelineNode>(
        /*
          Map of nodes to their incoming edges.
         */
        Map<N, List<Edge<?, N>>> incomingEdges,

        /*
          Map of nodes to their outgoing edges.
         */
        Map<N, List<Edge<?, N>>> outgoingEdges
) {
}