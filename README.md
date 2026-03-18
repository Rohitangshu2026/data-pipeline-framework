# Data Pipeline Framework

A lightweight **XML-driven data pipeline framework** written in Java.

The system parses pipeline definitions from XML, validates them against an XSD schema, builds a dependency graph of stages, and executes the pipeline in **topologically ordered stages**.

---

# Features

- XML-based pipeline configuration
- XSD schema validation
- Semantic validation
- Dependency resolution between stages
- Topological execution order
- Modular architecture
- Extensible execution engine

---

# Project Architecture

The framework follows a layered processing pipeline:

```
XML Configuration
        ↓
XSD Validation
        ↓
JAXB Parsing
        ↓
Semantic Validation
        ↓
Configuration Normalization
        ↓   
Dependency Graph Construction
        ↓
Execution Engine
```

---
# Data Model

## XML Schema (Tree Representation)
```
job (id)
└── stage* (id, pre_req?)
    ├── on_error? (handling_strategy, retry_count?)
    └── task+
        ├── input (src)
        ├── action (type)
        └── output (src)
```

## Java Object Model
```
Job
└── Stages
    └── Tasks
        ├── Input
        ├── Action
        └── Output
```
---


# Pipeline Execution Flow

The following diagram shows the **complete lifecycle of a pipeline run**.

<p align="center">
  <img src="/images/pipeline-flowchart.png" width="300" height="700"/>
</p>

### Execution Steps

1. CLI starts the application
2. `Main.main()` receives the XML path
3. `Pipeline.run()` orchestrates execution
4. `JAXBPipelineParser` parses XML
5. XML validated against `job.xsd`
6. `Job` object graph is created
7. `SemanticValidator` validates pipeline semantics
8. `ConfigNormalizer` resolves dependencies
9. Dependency graph is built
10. Execution levels are computed
11. Pipeline structure is printed
12. `PipelineExecutor` runs stages level by level

---
## Validation Layers

### 1. XSD Validation (Structure)
- Required tags
- Attribute validation
- Correct XML structure

### 2. Semantic Validation (Logic)
- Unique stage IDs
- Valid dependencies
- Tasks must exist
- Input, Action, Output required
- Valid on_error strategy

### 3. Runtime Validation
- Cycle detection during DAG construction

---
## DAG Execution Logic

The framework uses Kahn’s Algorithm (Topological Sort).

Algorithm:

1. Compute indegree of each stage
2. Add all stages with indegree = 0 to a queue
3. Process level by level
4. Reduce indegree of dependent stages
5. Add new zero-indegree stages to queue
6. If processed nodes are not equal to total nodes, a cycle exists
---

### Key Interactions
````
CLI → Main → Pipeline

Pipeline → Parser
Parser → XML Schema Validation
Parser → Job Object

Pipeline → SemanticValidator
Pipeline → ConfigNormalizer

Pipeline → PipelineExecutor

PipelineExecutor → Job.getExecutionLevels()
PipelineExecutor → Stage execution

Stage → Task execution
````

---

# UML Class Diagram

The class diagram shows the **core object model of the pipeline system**.

![UML Diagram](/images/uml-class-diagram.png)

---

## Core Components

### Main
- Entry point
- Handles CLI input and exceptions

### Pipeline
- Orchestrates execution
- Flow: parse -> validate -> normalize -> execute

### Job
- Root pipeline object
- Maintains stages and stage map
- Builds DAG and execution levels

### Stage
- Represents a DAG node
- Contains tasks, dependencies, and error handling configuration

### Task
- Execution unit consisting of input, action, and output

### JAXBPipelineParser
- Converts XML to object graph
- Applies XSD validation

### SemanticValidator
- Ensures logical correctness of pipeline

### ConfigNormalizer
- Resolves dependencies and prepares configuration

### PipelineExecutor
- Executes pipeline stage by stage

---

# Project Structure
````
data-pipeline-framework/
│
├── src/main/java/org/example/datapipeline/
│   │
│   ├── cli/
│   │   └── Pipeline.java   → CLI runner
│   │
│   ├── config/     → Core domain model
│   │   ├── Job.java
│   │   ├── Stage.java
│   │   ├── Task.java
│   │   ├── Input.java
│   │   ├── Output.java
│   │   ├── Action.java
│   │   └── OnError.java
│   │
│   ├── parser/
│   │   └── JAXBPipelineParser.java     → XML → Object
│   │
│   ├── validator/
│   │   └── SemanticValidator.java      → Logical validation
│   │
│   ├── util/
│   │   └── ConfigNormalizer.java       → Dependency normalization
│   │
│   ├── executor/
│   │   └── PipelineExecutor.java       → Execution engine
│   │
│   ├── exception/
│   │   └── PipelineValidationException.java
│   │
│   └── Main.java   → Entry point
│
├── src/main/resources/
│   ├── job.xsd     → Schema definition
│   └── pipeline_*.xml  → Test configs
│
├── src/test/
│   └── PipelineTest.java   → Full test suite
│
├── images/    
├── pom.xml
└── README.md

````


---

## Testing

Covered scenarios:

### Valid Pipelines
- Simple
- Parallel
- Diamond DAG
- Fanout
- Large scale pipelines

### Invalid Pipelines
- Cycles
- Duplicate stages
- Missing dependencies
- Invalid configurations

### Additional
- CLI execution
- DAG correctness

---

## How to Run
```bash
mvn clean install
java -cp target/classes org.example.datapipeline.Main <pipeline.xml>
```