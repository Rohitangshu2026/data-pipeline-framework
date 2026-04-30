package org.example.datapipeline.executor.action;


import org.example.datapipeline.executor.context.ExecutionContext;
import org.example.datapipeline.config.action.Method;


import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;


/**
 * Action executor that runs an external bash script as a pipeline task.
 *
 * <p>This executor bridges the declarative XML pipeline with arbitrary shell scripts,
 * enabling complex reporting, notification, data export, or any other operation that
 * is easier to express in bash than in Java.
 *
 * <h2>Method: {@code run}</h2>
 * <p>The only currently supported method is {@code run}. It assembles and launches a
 * {@code bash <script> <inputFile> [arg1 arg2 ...] [outputFile]} command using
 * {@link ProcessBuilder} with inherited I/O (script stdout/stderr appear on the terminal):
 * <ol>
 *   <li>The {@code script} parameter specifies the path to the shell script.</li>
 *   <li>The resolved input {@code src} path is passed as the first positional argument.</li>
 *   <li>Any additional parameters named {@code arg1}, {@code arg2}, etc. are appended
 *       in lexicographic order (so {@code arg1} < {@code arg2} < {@code arg10} — use
 *       zero-padded names if order matters beyond 9 args).</li>
 *   <li>The resolved output {@code src} path is appended last (if an output is declared).</li>
 * </ol>
 *
 * <p>Execution blocks until the script process exits. A non-zero exit code is treated as
 * a fatal error and throws a {@link RuntimeException}, causing the stage to fail.
 *
 * <h2>Output Handling</h2>
 * <p>Because the script writes the output file itself, {@link #handlesOwnOutput()} returns
 * {@code true}. The framework therefore skips its normal post-execute
 * {@link org.example.datapipeline.config.output.Output#writeData} step, preventing the
 * script's output from being overwritten with the raw input CSV rows.
 *
 * <h2>Thread Safety</h2>
 * <p>This class is a shared singleton registered in
 * {@link org.example.datapipeline.executor.action.ActionRegistry}. The {@code methods} map
 * is immutable after construction, and {@link ProcessBuilder} creates a new process per
 * invocation, so the class is effectively thread-safe.
 */
public class BashAction implements ActionExecutor {

    private final Map<String, BashMethod> methods = new HashMap<>();

    /**
     * Registers the built-in {@code run} method handler.
     */
    public BashAction() {
        methods.put("run", this::run);
    }

    /**
     * Dispatches to the appropriate bash method handler.
     *
     * <p>Looks up the method name (lowercased) in the internal methods map and delegates.
     * Currently only {@code "run"} is supported.
     *
     * @param ctx the execution context with input, output, and method parameters
     * @throws RuntimeException if the method name is not registered
     */
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

    /**
     * Functional interface for bash method handlers, enabling clean method references
     * and future extensibility without changing the dispatch mechanism.
     */
    @FunctionalInterface
    interface BashMethod {
        void apply(ExecutionContext ctx);
    }

    /**
     * Builds and executes the bash command for the {@code run} method.
     *
     * <p>Command format:
     * <pre>bash &lt;script&gt; &lt;inputSrc&gt; [arg1] [arg2] ... [outputSrc]</pre>
     *
     * <p>The process inherits the current process's stdin, stdout, and stderr so that
     * script output (ANSI-coloured reports, etc.) is visible in the terminal. The method
     * blocks until the script exits.
     *
     * @param ctx execution context providing the input path, output path, and method params
     * @throws RuntimeException if {@code script} param is missing, the script exits with a
     *                          non-zero code, or an I/O/interrupt error occurs
     */
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

    /**
     * Returns {@code "bash"} – the action type string used in the pipeline XML
     * ({@code <action type="bash">}).
     */
    @Override
    public String getType() {
        return "bash";
    }

    /**
     * Returns {@code true} because bash scripts write their output file directly.
     *
     * <p>Without this override, the framework would open the output file after the script
     * finishes and overwrite its contents with the unconsumed input CSV rows. By returning
     * {@code true}, the framework instead drains the input iterator (for row-count metrics
     * only) and leaves the file written by the script untouched.
     *
     * @return always {@code true}
     */
    @Override
    public boolean handlesOwnOutput() {
        return true;
    }
}