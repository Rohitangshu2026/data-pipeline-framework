package org.example.datapipeline.exception;

public class PipelineValidationException extends RuntimeException {
    public PipelineValidationException(String message) {
        super(message);
    }
}