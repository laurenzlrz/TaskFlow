# TaskFlow

A lightweight Java framework for building and executing DAG-based pipelines with parallel execution and built-in logging. 

## Overview

TaskFlow is designed to simplify the creation and execution of complex task pipelines using Directed Acyclic Graphs (DAGs). It enables developers to define dependencies between tasks and automatically executes them in the correct order with support for parallel execution where possible.

## Features

- **DAG-Based Pipeline Execution**: Define tasks and their dependencies in a directed acyclic graph structure
- **Parallel Execution**: Automatically executes independent tasks in parallel for optimal performance
- **Built-in Logging**: Comprehensive logging system to track pipeline execution and debug issues
- **Lightweight**: Minimal dependencies and overhead
- **Type-Safe**: Leverages Java's type system for compile-time safety
- **Easy to Use**: Simple and intuitive API for defining tasks and pipelines

## Requirements

- Java 11 or higher
- No external dependencies (core functionality)

## Building from Source

```bash
git clone https://github.com/laurenzlrz/TaskFlow.git
cd TaskFlow
mvn clean install
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
