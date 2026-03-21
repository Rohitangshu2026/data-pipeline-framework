package org.example.datapipeline.executor.action;

import org.example.datapipeline.config.input.Input;
import org.example.datapipeline.executor.context.ExecutionContext;
import org.example.datapipeline.config.action.Method;
import org.example.datapipeline.config.output.Output;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Executes bash-based actions within the data pipeline.
 *
 * This executor is responsible for running shell scripts defined in the
 * pipeline configuration. It retrieves the input script path, output
 * destination, and method details from the execution context, and invokes
 * the script using a system bash process.
 *
 * Supports method-based execution, where different bash operations can be
 * mapped and invoked dynamically. Currently, it provides a default "run"
 * method to execute a script with the given input and output paths.
 *
 * Ensures that the script execution is blocking and validates the exit
 * status. Any non-zero exit code or execution failure results in a runtime
 * exception.
 */
public class BashAction implements ActionExecutor {

    private final Map<String, BashMethod> methods = new HashMap<>();

    public BashAction() {
        methods.put("run", this::run);
    }

    @Override
    public void execute(ExecutionContext ctx) {

        Method method = ctx.getMethod();

        String methodName = method.getName().toLowerCase();

        BashMethod fn = methods.get(methodName);

        if (fn == null){
            throw new RuntimeException("Unsupported bash method: " + methodName);
        }

        fn.apply(ctx);
    }

    @FunctionalInterface
    interface BashMethod {
        void apply(ExecutionContext ctx);
    }

    private void run(ExecutionContext ctx) {

        String inputData = ctx.getInput().getSrc();
        String output = ctx.getOutput() != null ? ctx.getOutput().getSrc() : null;

        Map<String, String> params = ctx.getMethod().getParamMap();

        String script = params.get("script");

        if (script == null || script.isBlank()) {
            throw new RuntimeException("Missing 'script' param for bash action");
        }

        List<String> command = new ArrayList<>();
        command.add("bash");
        command.add(script);

        // 🔹 pass input data first
        command.add(inputData);

        // 🔹 ordered args (arg1, arg2, ...)
        params.entrySet().stream()
                .filter(e -> e.getKey().startsWith("arg"))
                .sorted(Map.Entry.comparingByKey())
                .forEach(e -> command.add(e.getValue()));

        // 🔹 optional output last
        if (output != null) {
            command.add(output);
        }

        System.out.println("[BASH] Command: " + String.join(" ", command));

        try {
            ProcessBuilder pb = new ProcessBuilder(command);
            pb.inheritIO();

            Process p = pb.start();
            int exit = p.waitFor();

            if (exit != 0) {
                throw new RuntimeException("Script failed with exit code: " + exit);
            }

        } catch (IOException | InterruptedException e) {
            throw new RuntimeException("Bash execution failed", e);
        }
    }

    @Override
    public String getType() {
        return "bash";
    }
}