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

# Project Structure
````
src/main/java/org/example/datapipeline

cli/ → CLI entrypoint
config/ → Pipeline configuration object model
parser/ → XML → Object graph conversion
validator/ → Semantic validation
util/ → Configuration normalization
executor/ → Pipeline execution engine
````


---

# Pipeline Execution Flow

The following diagram shows the **complete lifecycle of a pipeline run**.

![Pipeline Flow](/images/pipeline-flowchart.png)

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

# Runtime Sequence Diagram

The following sequence diagram illustrates **runtime interaction between components**.

![Runtime Sequence](/images/runtime-sequence-diagram.png)

### Key Interactions
````
CLI         →   Main    →   Pipeline
Pipeline    →   Parser
Parser      →   XML Schema Validation
Parser      →   Job Object

Pipeline    →   SemanticValidator
Pipeline    →   ConfigNormalizer

Pipeline    →   PipelineExecutor

PipelineExecutor    →   Job.getExecutionLevels()
PipelineExecutor    →   Stage execution

Stage       →     Task execution
````

---

# UML Class Diagram

The class diagram shows the **core object model of the pipeline system**.

![UML Diagram](/images/uml-class-diagram.png)


