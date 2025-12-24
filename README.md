# TaskFlow

A lightweight Java framework for building and executing DAG-based pipelines with parallel execution and built-in logging. 

## Overview

TaskFlow is designed to simplify the creation and execution of complex task pipelines using Directed Acyclic Graphs (DAGs). It enables developers to define dependencies between tasks and automatically executes them in the correct order with support for parallel execution where possible.

## Features

- **DAG-Based Pipeline Execution**: Define tasks and their dependencies in a directed acyclic graph structure
- **Parallel Execution**:  Automatically executes independent tasks in parallel for optimal performance
- **Built-in Logging**: Comprehensive logging system to track pipeline execution and debug issues
- **Lightweight**: Minimal dependencies and overhead
- **Type-Safe**: Leverages Java's type system for compile-time safety
- **Easy to Use**: Simple and intuitive API for defining tasks and pipelines

## Installation

### Maven

```xml
<dependency>
    <groupId>io.github.laurenzlrz</groupId>
    <artifactId>taskflow</artifactId>
    <version>1.0.0</version>
</dependency>
```

### Gradle

```gradle
implementation 'io.github.laurenzlrz:taskflow:1.0.0'
```

## Quick Start

Here's a simple example of creating and executing a task pipeline:

```java
import io.github.taskflow.*;

public class Example {
    public static void main(String[] args) {
        // Create tasks
        Task task1 = new Task("task1", () -> {
            System.out.println("Executing task 1");
            return "Result 1";
        });
        
        Task task2 = new Task("task2", () -> {
            System.out.println("Executing task 2");
            return "Result 2";
        });
        
        Task task3 = new Task("task3", () -> {
            System.out. println("Executing task 3 (depends on 1 and 2)");
            return "Result 3";
        });
        
        // Build pipeline with dependencies
        Pipeline pipeline = new Pipeline. Builder()
            .addTask(task1)
            .addTask(task2)
            .addTask(task3, task1, task2) // task3 depends on task1 and task2
            .build();
        
        // Execute the pipeline
        PipelineResult result = pipeline.execute();
        
        if (result.isSuccess()) {
            System.out. println("Pipeline executed successfully!");
        }
    }
}
```

## Core Concepts

### Tasks

Tasks are the basic units of work in TaskFlow. Each task: 
- Has a unique identifier
- Executes a specific action
- Can depend on other tasks
- Returns a result upon completion

### Pipeline

A Pipeline is a collection of tasks organized as a DAG: 
- Validates that no circular dependencies exist
- Determines the optimal execution order
- Executes tasks in parallel when dependencies allow
- Collects and returns results

### Dependencies

Dependencies define the execution order:
- A task only executes after all its dependencies complete successfully
- Tasks without dependencies execute immediately
- Independent tasks can run in parallel

## Advanced Usage

### Custom Task Types

```java
public class DataProcessingTask extends Task {
    public DataProcessingTask(String id, DataSource source) {
        super(id, () -> processData(source));
    }
    
    private static Object processData(DataSource source) {
        // Custom processing logic
        return processedData;
    }
}
```

### Error Handling

```java
Pipeline pipeline = new Pipeline.Builder()
    .addTask(task1)
    .addTask(task2)
    .setErrorHandler((task, exception) -> {
        System.err.println("Task " + task. getId() + " failed: " + exception.getMessage());
    })
    .build();
```

### Logging Configuration

```java
Pipeline pipeline = new Pipeline.Builder()
    .addTask(task1)
    .setLogLevel(LogLevel.DEBUG)
    .enableDetailedLogging(true)
    .build();
```

## Requirements

- Java 11 or higher
- No external dependencies (core functionality)

## Building from Source

```bash
git clone https://github.com/laurenzlrz/TaskFlow. git
cd TaskFlow
mvn clean install
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Author

**laurenzlrz** - [GitHub Profile](https://github.com/laurenzlrz)

## Roadmap

- [ ] Add support for conditional task execution
- [ ] Implement task retry mechanisms
- [ ] Add metrics and monitoring capabilities
- [ ] Support for distributed execution
- [ ] Visual pipeline builder and monitor

## Support

For questions, issues, or feature requests, please [open an issue](https://github.com/laurenzlrz/TaskFlow/issues) on GitHub.
```
