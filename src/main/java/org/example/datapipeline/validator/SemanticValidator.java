package org.example.datapipeline.validator;

import org.example.datapipeline.config.Job;
import org.example.datapipeline.config.Stage;
import org.example.datapipeline.config.OnError;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Validates the logical correctness of a parsed pipeline configuration.
 *
 * This validator ensures that the pipeline is not just structurally valid
 * (handled by XSD), but also semantically meaningful and executable.
 *
 * It performs the following checks:
 *
 * 1. Stage IDs
 *    - Each stage must have a non-empty identifier
 *    - No duplicate stage IDs are allowed
 *
 * 2. Dependencies
 *    - All dependencies must refer to existing stages
 *
 * 3. Tasks
 *    - Every stage must contain at least one task
 *    - Each task must define input, action, and output
 *
 * 4. Error Handling (on_error)
 *    - handling_strategy must be one of: retry, abort, proceed
 *    - retry strategy requires a non-negative retry_count
 *    - retry_count must not be defined for non-retry strategies
 *
 * The validator follows a fail-fast approach and throws an exception
 * immediately when an invalid configuration is encountered.
 *
 * This step is executed after parsing and before DAG construction.
 */
public class SemanticValidator {

    /**
     * Runs all semantic validation checks on the given job.
     */
    public static void validate(Job job) {
        validateStageIds(job);
        validateDependencies(job);
        validateTasks(job);
        validateOnError(job);
    }

    /**
     * Ensures all stages have unique and non-empty IDs.
     */
    private static void validateStageIds(Job job) {
        Set<String> stageIds = new HashSet<>();
        for (Stage stage : job.getStages()) {
            if (stage.getId() == null || stage.getId().isBlank()) {
                throw new RuntimeException("Stage id cannot be null or empty");
            }
            if (!stageIds.add(stage.getId())) {
                throw new RuntimeException("Duplicate stage id: " + stage.getId());
            }
        }
    }

    /**
     * Ensures that all declared dependencies refer to valid stages.
     */
    private static void validateDependencies(Job job) {
        Set<String> stageIds = new HashSet<>();
        for (Stage s : job.getStages()) {
            stageIds.add(s.getId());
        }
        for (Stage stage : job.getStages()) {

            for (String dep : stage.getDependencies()) {

                if (!stageIds.contains(dep)) {
                    throw new RuntimeException(
                            "Stage '" + stage.getId() +
                                    "' depends on unknown stage '" + dep + "'"
                    );
                }
            }
        }
    }

    /**
     * Ensures each stage contains valid tasks with input, action, and output.
     */
    private static void validateTasks(Job job) {
        for (Stage stage : job.getStages()) {
            if (stage.getTasks() == null || stage.getTasks().isEmpty()) {
                throw new RuntimeException(
                        "Stage '" + stage.getId() + "' has no tasks"
                );
            }
            stage.getTasks().forEach(task -> {
                if (task.getInput() == null) {
                    throw new RuntimeException(
                            "Task in stage '" + stage.getId() + "' missing input"
                    );
                }
                if (task.getAction() == null || task.getAction().getType() == null) {
                    throw new RuntimeException(
                            "Task in stage '" + stage.getId() + "' missing action"
                    );
                }
                if (task.getOutput() == null) {
                    throw new RuntimeException(
                            "Task in stage '" + stage.getId() + "' missing output"
                    );
                }
                validateMethodParams(task, stage.getId());
            });
        }
    }

    private static void validateMethodParams(org.example.datapipeline.config.Task task, String stageId) {
        org.example.datapipeline.config.action.Action action = task.getAction();
        if (action == null || action.getType() == null) return;
        
        org.example.datapipeline.config.action.Method method = action.getMethod();
        if (method == null || method.getName() == null) return;
        
        java.util.Map<String, String> params = method.getParamMap();
        String actionType = action.getType().toLowerCase();
        String methodName = method.getName().toLowerCase();
        
        if ("transform".equals(actionType)) {
            switch (methodName) {
                case "derive" -> requireParams(params, stageId, methodName, "new_column", "formula");
                case "drop_nulls" -> requireParams(params, stageId, methodName, "columns");
                case "fill_nulls" -> requireParams(params, stageId, methodName, "column", "value");
                case "sort" -> requireParams(params, stageId, methodName, "column", "order");
                case "limit" -> requireParams(params, stageId, methodName, "count");
                case "normalize" -> requireParams(params, stageId, methodName, "column");
                case "scale" -> requireParams(params, stageId, methodName, "column");
                case "filter" -> requireParams(params, stageId, methodName, "column", "operator", "value");
                case "map" -> requireParams(params, stageId, methodName, "column", "operation", "value");
                case "select" -> requireParams(params, stageId, methodName, "columns");
                case "aggregate" -> requireParams(params, stageId, methodName, "group_by", "column", "operation");
                case "max" -> requireParams(params, stageId, methodName, "column");
            }
        } else if ("join".equals(actionType)) {
            if (!params.containsKey("left_key") || !params.containsKey("right_key") || (!params.containsKey("right_src") && !params.containsKey("right_ref"))) {
                throw new RuntimeException("Missing parameters for join. Required: left_key, right_key, and (right_src or right_ref)");
            }
            String joinType = params.getOrDefault("join_type", "inner").toLowerCase();
            if (!Set.of("inner", "left", "right", "full").contains(joinType)) {
                throw new RuntimeException("Invalid join_type: " + joinType);
            }
            String joinStrategy = params.getOrDefault("join_strategy", "hash").toLowerCase();
            if (!Set.of("hash", "sort_merge").contains(joinStrategy)) {
                throw new RuntimeException("Invalid join_strategy: " + joinStrategy);
            }
            if ("sort_merge".equals(joinStrategy) && !"inner".equals(joinType)) {
                throw new RuntimeException(
                    "Stage '" + stageId + "': join_strategy 'sort_merge' only supports " +
                    "join_type 'inner'. Use 'hash' for outer joins."
                );
            }
        } else if ("bash".equals(actionType) && "run".equals(methodName)) {
            requireParams(params, stageId, methodName, "script");
        }
    }

    private static void requireParams(java.util.Map<String, String> params, String stageId, String methodName, String... required) {
        for (String req : required) {
            String val = params.get(req);
            if (val == null || val.isBlank()) {
                throw new RuntimeException(String.format(
                        "Task in stage '%s' with method '%s' is missing required parameter: '%s'",
                        stageId, methodName, req
                ));
            }
        }
    }

    /**
     * Validates error handling configuration for each stage.
     */
    private static void validateOnError(Job job) {

        for (Stage stage : job.getStages()) {
            OnError onError = stage.getOnError();
            if (onError == null) continue;
            String strategy = onError.getHandlingStrategy();
            Integer retry = onError.getRetryCount();
            if (strategy == null || strategy.isBlank()) {
                throw new RuntimeException("Stage '" + stage.getId() + "' has invalid handling_strategy");
            }
            if (!List.of("retry", "abort", "proceed").contains(strategy)) {
                throw new RuntimeException("Stage '" + stage.getId() +
                                "' has unknown handling_strategy: " + strategy);
            }
            if ("retry".equals(strategy)) {
                if (retry == null) {
                    throw new RuntimeException("Stage '" + stage.getId() +
                                    "' requires retry_count for retry strategy");
                }
                if (retry < 0) {
                    throw new RuntimeException("Stage '" + stage.getId() +
                                    "' has negative retry_count");
                }
            }
            if (!"retry".equals(strategy) && retry != null) {
                throw new RuntimeException("Stage '" + stage.getId() +
                                "' should not define retry_count when strategy is " + strategy
                );
            }
        }
    }
}