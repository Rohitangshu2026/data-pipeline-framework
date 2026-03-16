package org.example.datapipeline.validator;

import org.example.datapipeline.config.Job;
import org.example.datapipeline.config.Stage;

import java.util.HashSet;
import java.util.Set;

public class SemanticValidator {

    public static void validate(Job job) {

        validateStageIds(job);
        validateDependencies(job);
        validateTasks(job);

    }

    private static void validateStageIds(Job job) {

        Set<String> stageIds = new HashSet<>();

        for(Stage stage : job.getStages()) {

            if(stageIds.contains(stage.getId())) {
                throw new RuntimeException("Duplicate stage id: " + stage.getId());
            }

            stageIds.add(stage.getId());
        }
    }

    private static void validateDependencies(Job job) {

        Set<String> stageIds = new HashSet<>();

        for(Stage s : job.getStages()) {
            stageIds.add(s.getId());
        }

        for(Stage stage : job.getStages()) {

            for(String dep : stage.getDependencies()) {

                if(!stageIds.contains(dep)) {
                    throw new RuntimeException(
                            "Stage '" + stage.getId() +
                                    "' depends on unknown stage '" + dep + "'"
                    );
                }
            }
        }
    }

    private static void validateTasks(Job job) {

        for(Stage stage : job.getStages()) {

            if(stage.getTasks() == null || stage.getTasks().isEmpty()) {
                throw new RuntimeException(
                        "Stage '" + stage.getId() + "' has no tasks"
                );
            }
        }
    }
}