package org.example.datapipeline.executor.action;

import org.example.datapipeline.config.input.Input;
import org.example.datapipeline.config.output.Output;
import org.example.datapipeline.config.action.Method;
import org.example.datapipeline.executor.context.ExecutionContext;

import java.util.HashMap;
import java.util.Map;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Executes transformation actions within the data pipeline.
 *
 * This executor applies data transformation logic based on the method
 * specified in the execution context. It supports dynamic method
 * dispatch, allowing different transformation operations to be added
 * and invoked by name.
 *
 * Features:
 * - Filter:
 *   Filters rows from CSV input based on a condition applied to a
 *   specified column. Supports numeric comparisons (>, <, =, >=, <=)
 *   and string equality checks.
 *
 * Ensures proper error handling for invalid methods, missing columns,
 * and execution failures, with contextual logging for easier debugging.
 */
public class TransformAction implements ActionExecutor{

    private final Map<String, TransformMethod> methods = new HashMap<>();

    public TransformAction(){
        methods.put("filter", this::filter);
    }

    @Override
    public void execute(ExecutionContext ctx){

        Method method = ctx.getMethod();

        TransformMethod fn = methods.get(method.getName().toLowerCase());

        if (fn == null){
            throw new RuntimeException("Unsupported transform method: " + method.getName());
        }
        fn.apply(ctx);
    }

    @FunctionalInterface
    interface TransformMethod{
        void apply(ExecutionContext ctx);
    }

    private void filter(ExecutionContext ctx){

        Input input = ctx.getInput();
        Output output = ctx.getOutput();
        Method method = ctx.getMethod();

        Map<String, String> params = method.getParamMap();

        String column = params.get("column");
        String operator = params.get("operator");
        String value = params.get("value");

        String stageId = (String) ctx.getMetadata().get("stageId");

        String log = String.format(
                "[%s] FILTER | input=%s | output=%s | %s %s %s",
                stageId,
                input.getSrc(),
                output.getSrc(),
                column,
                operator,
                value
        );
        System.out.println(log);

        try (
                BufferedReader reader = new BufferedReader(new FileReader(input.getSrc()));
                BufferedWriter writer = new BufferedWriter(new FileWriter(output.getSrc()))
        ){
            String header = reader.readLine();
            if (header == null) return;

            writer.write(header);
            writer.newLine();

            String[] cols = header.split(",");
            int colIndex = -1;

            for (int i = 0; i < cols.length; i++) {
                if (cols[i].trim().equals(column)) {
                    colIndex = i;
                    break;
                }
            }

            if (colIndex == -1) {
                throw new RuntimeException("Column not found: " + column);
            }
            String line;

            while ((line = reader.readLine()) != null) {

                String[] parts = line.split(",");

                if (colIndex >= parts.length) continue;

                String cell = parts[colIndex].trim();

                if (evaluate(cell, operator, value)) {
                    writer.write(line);
                    writer.newLine();
                }
            }

            System.out.println(
                    "[" + ctx.getMetadata().get("stageId") + "] Filter completed successfully."
            );

        } catch (Exception e) {
            throw new RuntimeException(
                    "[" + stageId + "] Filter failed for input=" + input.getSrc(), e);
        }
    }

    private boolean evaluate(String cell, String operator, String value) {

        try {
            double c = Double.parseDouble(cell);
            double v = Double.parseDouble(value);

            return switch (operator) {
                case ">" -> c > v;
                case "<" -> c < v;
                case "=" -> c == v;
                case ">=" -> c >= v;
                case "<=" -> c <= v;
                default -> throw new RuntimeException("Invalid operator: " + operator);
            };

        } catch (NumberFormatException e) {
            return cell.equals(value);
        }
    }

    @Override
    public String getType() {
        return "transform";
    }
}