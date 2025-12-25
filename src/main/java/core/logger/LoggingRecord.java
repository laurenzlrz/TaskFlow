package core.logger;

import core.nodes.IPipelineNode;
import core.run_bundle.LoggingRunBundle;

/**
 * Record representing a logging record for a pipeline node.
 *
 * @param <N> the type of pipeline node
 */
public record LoggingRecord<N extends IPipelineNode>(
        /*
          The phase of the logging.
         */
        String phase,

        /*
          The pipeline node being logged.
         */
        N node,

        /*
          The logging run bundle.
         */
        LoggingRunBundle<N, ?, ?> bundle,

        /*
          The timestamp of the logging event.
         */
        long timestamp,

        /*
          The time difference since the last log.
         */
        long deltaTime,

        /*
          The used heap memory at the time of logging.
         */
        long usedHeapMemory,

        /*
          The used non-heap memory at the time of logging.
         */
        long usedNonHeapMemory,

        /*
          The free memory at the time of logging.
         */
        long freeMemory,

        /*
          The total memory at the time of logging.
         */
        long totalMemory,

        /*
          The maximum memory available at the time of logging.
         */
        long maxMemory,

        /*
          The number of active threads at the time of logging.
         */
        int activeThreads,

        /*
          The CPU time used by the current thread at the time of logging.
         */
        long currentThreadCpuTime,

        /*
          The name of the thread performing the logging.
         */
        String threadName,

        /*
          The number of available processors at the time of logging.
         */
        int availableProcessors
) {}