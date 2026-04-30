package org.example.datapipeline.exception;

/**
 * Unchecked exception thrown when a pipeline configuration fails structural or semantic validation.
 *
 * <p>This exception is raised by {@link org.example.datapipeline.parser.JAXBPipelineParser}
 * when the XML document violates the XSD schema constraints (structural validation), and may
 * also be thrown by
 * {@link org.example.datapipeline.validator.SemanticValidator} for logical violations
 * (e.g. unknown stage dependency, missing task action).
 *
 * <p>The message is designed to be human-readable and actionable, including the file path,
 * line/column numbers, and a description of the specific issue.
 *
 * <p>Extending {@link RuntimeException} (rather than a checked exception) avoids forcing
 * callers throughout the framework to declare or catch it; the pipeline entry point
 * ({@link org.example.datapipeline.Main}) catches all runtime exceptions at the top level.
 */
public class PipelineValidationException extends RuntimeException {

    /**
     * Creates a new validation exception with the given detail message.
     *
     * @param message a human-readable description of the validation failure, including
     *                the file, location, and nature of the problem where available
     */
    public PipelineValidationException(String message) {
        super(message);
    }
}