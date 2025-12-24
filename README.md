# TaskFlow

A lightweight Java framework for building and executing DAG-based pipelines with parallel execution and optimized thread management.

## Features

- **DAG-based Pipeline Execution**: Build complex workflows with task dependencies
- **Optimized Thread Management**: Uses `ThreadPoolExecutor` with `keepAliveTime` for efficient resource utilization
- **Auto-shutdown on Idle**: Automatically terminates threads after a configurable period of inactivity
- **Graceful Shutdown**: Properly manages worker lifecycle with graceful shutdown mechanisms
- **Parallel Execution**: Execute independent tasks concurrently while respecting dependencies
- **Lightweight**: Minimal dependencies, easy to integrate

## Key Benefits

The framework addresses common issues with thread pool management:

1. **No Resource Waste**: Unlike `FixedThreadPool`, threads don't stay alive forever consuming memory when idle
2. **Automatic Scaling**: Threads are created on-demand and terminated after idle timeout
3. **Configurable Lifecycle**: Full control over thread pool behavior with customizable timeouts
4. **Graceful Degradation**: Proper shutdown mechanisms ensure clean resource cleanup

## Quick Start

### Basic Usage

```java
// Create orchestrator with auto-shutdown after 5 minutes of inactivity
PipelineOrchestrator orchestrator = new PipelineOrchestrator();

// Create a bundle of tasks
ParallelRunBundle bundle = orchestrator.createBundle();

// Add tasks
bundle.addTask(new TaskNode("task-1") {
    @Override
    public void execute() throws Exception {
        System.out.println("Executing task 1");
        // Your task logic here
    }
});

// Execute the bundle
ParallelRunBundle.ExecutionResult result = orchestrator.executeBundle(bundle);

// Shutdown when done
orchestrator.shutdown();
```

### Custom Configuration

```java
// Configure thread pool and auto-shutdown
PipelineOrchestrator orchestrator = new PipelineOrchestrator(
    4,                      // core pool size
    8,                      // max pool size
    60L,                    // thread keep-alive time
    TimeUnit.SECONDS,       // keep-alive unit
    true,                   // enable auto-shutdown
    10L,                    // idle timeout
    TimeUnit.MINUTES        // idle timeout unit
);
```

### DAG with Dependencies

```java
TaskNode task1 = new TaskNode("root") {
    @Override
    public void execute() {
        // Root task
    }
};

TaskNode task2 = new TaskNode("child") {
    @Override
    public void execute() {
        // This runs after task1
    }
};
task2.addDependency(task1);

bundle.addTask(task1);
bundle.addTask(task2);
orchestrator.executeBundle(bundle);
```

## Architecture

### WorkerPool

The `WorkerPool` class is the core component managing thread execution:

- Uses `ThreadPoolExecutor` with configurable `keepAliveTime`
- Allows core threads to timeout (`allowCoreThreadTimeOut(true)`)
- Provides graceful and forced shutdown methods
- Supports auto-shutdown on idle with configurable timeout

### PipelineOrchestrator

The `PipelineOrchestrator` manages the overall pipeline lifecycle:

- Creates and manages multiple execution bundles
- Controls the WorkerPool lifecycle
- Provides statistics and monitoring capabilities
- Handles graceful shutdown of all resources

### ParallelRunBundle

The `ParallelRunBundle` executes tasks while respecting dependencies:

- Schedules tasks based on dependency resolution
- Executes independent tasks in parallel
- Tracks execution results and failures
- Manages task lifecycle

## Building and Testing

```bash
# Build the project
mvn clean compile

# Run tests
mvn test

# Run example
mvn exec:java -Dexec.mainClass="com.taskflow.Example"

# Package
mvn package
```

## Thread Management Details

### How It Works

1. **Thread Creation**: Threads are created on-demand up to `maxPoolSize`
2. **Keep-Alive**: Idle threads wait for `keepAliveTime` before terminating
3. **Core Timeout**: Even core threads timeout (unlike standard `FixedThreadPool`)
4. **Auto-Shutdown**: Optional automatic shutdown after idle period

### Resource Efficiency

- **Idle State**: Workers block on internal queue, consuming no CPU
- **Memory**: Threads terminate after idle timeout, freeing memory
- **Scalability**: Pool scales up/down based on workload

### Comparison with FixedThreadPool

| Feature | FixedThreadPool | TaskFlow WorkerPool |
|---------|----------------|---------------------|
| Thread Lifecycle | Threads never die | Threads timeout after idle |
| Resource Usage | Always consumes memory | Scales down when idle |
| Core Thread Timeout | No | Yes (configurable) |
| Auto-shutdown | No | Yes (optional) |
| Graceful Shutdown | Basic | Advanced with timeout |

## Requirements

- Java 11 or higher
- Maven 3.6 or higher

## License

See LICENSE file for details.
