package org.example.datapipeline.executor.action.transform;

import org.example.datapipeline.config.action.Method;
import org.example.datapipeline.executor.iterator.DataIterator;

import java.util.*;

public class DeriveStrategy implements TransformStrategy {

    @Override
    public DataIterator apply(DataIterator input, Method method) {

        Map<String, String> params = method.getParamMap();
        String newColumn = params.get("new_column");
        String formula = params.get("formula");

        if (newColumn == null || formula == null) {
            throw new RuntimeException("Missing params for derive");
        }

        return new DataIterator() {

            String[] header;
            boolean headerProcessed = false;
            Map<String, Integer> colIndexMap = new HashMap<>();

            @Override
            public boolean hasNext() {
                return input.hasNext();
            }

            @Override
            public String[] next() {
                if (!headerProcessed) {
                    String[] originalHeader = input.next();
                    header = Arrays.copyOf(originalHeader, originalHeader.length + 1);
                    header[header.length - 1] = newColumn;
                    for (int i = 0; i < originalHeader.length; i++) {
                        colIndexMap.put(originalHeader[i].trim(), i);
                    }
                    headerProcessed = true;
                    return header;
                }

                String[] row = input.next();
                String[] newRow = Arrays.copyOf(row, header.length);

                try {
                    double result = evaluateFormula(formula, row, colIndexMap);
                    newRow[newRow.length - 1] = String.valueOf(result);
                } catch (Exception e) {
                    newRow[newRow.length - 1] = ""; // default on error
                }

                return newRow;
            }
        };
    }

    private double evaluateFormula(String formula, String[] row, Map<String, Integer> colIndexMap) {
        List<String> tokens = tokenize(formula);
        List<String> rpn = toRPN(tokens);
        return evalRPN(rpn, row, colIndexMap);
    }

    private List<String> tokenize(String expr) {
        List<String> tokens = new ArrayList<>();
        StringBuilder num = new StringBuilder();

        for (char c : expr.toCharArray()) {
            if (Character.isWhitespace(c)) continue;

            if (Character.isLetterOrDigit(c) || c == '.' || c == '_') {
                num.append(c);
            } else {
                if (num.length() > 0) {
                    tokens.add(num.toString());
                    num.setLength(0);
                }
                tokens.add(String.valueOf(c));
            }
        }

        if (num.length() > 0) {
            tokens.add(num.toString());
        }

        return tokens;
    }

    private List<String> toRPN(List<String> tokens) {
        List<String> output = new ArrayList<>();
        Stack<String> ops = new Stack<>();

        Map<String, Integer> prec = Map.of(
                "+", 1,
                "-", 1,
                "*", 2,
                "/", 2
        );

        for (String token : tokens) {

            if (isNumberOrColumn(token)) {
                output.add(token);
            }

            else if ("+-*/".contains(token)) {
                while (!ops.isEmpty() && !ops.peek().equals("(") &&
                        prec.getOrDefault(ops.peek(), 0) >= prec.get(token)) {
                    output.add(ops.pop());
                }
                ops.push(token);
            }

            else if (token.equals("(")) {
                ops.push(token);
            }

            else if (token.equals(")")) {
                while (!ops.isEmpty() && !ops.peek().equals("(")) {
                    output.add(ops.pop());
                }
                ops.pop(); // remove "("
            }
        }

        while (!ops.isEmpty()) {
            output.add(ops.pop());
        }

        return output;
    }

    private double evalRPN(List<String> rpn, String[] row, Map<String, Integer> colIndexMap) {
        Stack<Double> stack = new Stack<>();

        for (String token : rpn) {

            if ("+-*/".contains(token)) {
                double b = stack.pop();
                double a = stack.pop();

                switch (token) {
                    case "+" -> stack.push(a + b);
                    case "-" -> stack.push(a - b);
                    case "*" -> stack.push(a * b);
                    case "/" -> stack.push(a / b);
                }
            } else {
                stack.push(getValue(token, row, colIndexMap));
            }
        }

        return stack.pop();
    }

    private boolean isNumberOrColumn(String token) {
        return !(token.equals("+") || token.equals("-") ||
                token.equals("*") || token.equals("/") ||
                token.equals("(") || token.equals(")"));
    }

    private double getValue(String token, String[] row, Map<String, Integer> colIndexMap) {
        Integer idx = colIndexMap.get(token);
        if (idx != null && idx < row.length) {
            try { return Double.parseDouble(row[idx]); } catch (Exception e) { return 0; }
        }
        try { return Double.parseDouble(token); } catch (Exception e) { return 0; }
    }
}
