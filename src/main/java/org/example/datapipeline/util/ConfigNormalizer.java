package org.example.datapipeline.util;

import org.example.datapipeline.config.Job;
import org.example.datapipeline.config.Stage;

/**
 * Utility responsible for normalizing pipeline configuration objects
 * after they are parsed from the XML definition.
 *
 * Normalization prepares the configuration for further processing by:
 * - Converting stage dependency strings into structured dependency sets
 * - Building lookup maps for efficient stage access
 *
 * This step ensures the pipeline configuration is in a consistent
 * and ready-to-use state before validation, DAG construction, or execution.
 */
public class ConfigNormalizer {

    /**
     * Normalizes the parsed Job configuration.
     *
     * This method resolves stage dependencies and prepares
     * lookup structures required for downstream processing.
     *
     * @param job the parsed pipeline job configuration
     */
    public static void normalize(Job job) {

        // convert pre_req → dependencies
        for(Stage stage : job.getStages()) {
            stage.normalizeDependencies();
        }

        // build stage lookup map
        job.buildStageMap();
    }
}