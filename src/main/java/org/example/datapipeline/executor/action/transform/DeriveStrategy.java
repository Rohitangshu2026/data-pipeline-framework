package org.example.datapipeline.executor.action.transform;

import org.example.datapipeline.config.action.Method;
import org.example.datapipeline.executor.iterator.DataIterator;

import java.util.*;

/**
 * Transform strategy that appends a new computed column to each row using an arithmetic formula.
 *
 * <p>Required method parameters:
 * <ul>
 *   <li>{@code new_column} – name of the column to append to the output</li>
 *   <li>{@code formula}    – arithmetic expression referencing existing column names and/or
 *       numeric literals (e.g. {@code "right_max_price - right_min_price"},
 *       {@code "sum_price / right_count_product_id"})</li>
 * </ul>
 *
 * <h2>Formula Language</h2>
 * <p>The formula supports a subset of arithmetic:
 * <ul>
 *   <li>Operators: {@code +}, {@code -}, {@code *}, {@code /}</li>
 *   <li>Grouping with parentheses: {@code (a + b) * c}</li>
 *   <li>Column names: any identifier composed of letters, digits, {@code .}, and {@code _}.
 *       Column names that include underscores (e.g. {@code right_count_product_id}) are
 *       fully supported.</li>
 *   <li>Numeric literals: integer or decimal constants</li>
 * </ul>
 *
 * <h2>Evaluation</h2>
 * <p>The formula is evaluated using a two-pass implementation:
 * <ol>
 *   <li><b>Tokenise</b> ({@link #tokenize}) – scans the formula character-by-character,
 *       collecting runs of identifier/digit characters into token strings and emitting
 *       operator characters as single-char tokens.</li>
 *   <li><b>Shunting-yard</b> ({@link #toRPN}) – converts the infix token list to
 *       Reverse Polish Notation using Dijkstra's shunting-yard algorithm, respecting
 *       operator precedence ({@code * /} > {@code + -}) and parentheses.</li>
 *   <li><b>RPN evaluation</b> ({@link #evalRPN}) – evaluates the RPN list with a
 *       {@code double} stack, resolving column name tokens by index lookup and numeric
 *       literal tokens by direct parse.</li>
 * </ol>
 *
 * <p>If evaluation fails (e.g. division by zero, missing column, non-numeric cell), the
 * new column cell is set to an empty string for that row rather than aborting.
 *
 * <p>The returned iterator is <em>lazy</em>: header expansion and formula evaluation happen
 * row-by-row during iteration with O(1) additional memory per row.
 */
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

    /**
     * Evaluates the formula against a single data row.
     *
     * <p>Delegates through tokenise → shunting-yard → RPN evaluation in sequence.
     *
     * @param formula     the raw formula string from the method parameters
     * @param row         the current data row (parallel to the column-index map)
     * @param colIndexMap pre-built mapping of column name → column index in {@code row}
     * @return the computed double result
     * @throws Exception if evaluation fails (caller converts to empty string)
     */
    private double evaluateFormula(String formula, String[] row, Map<String, Integer> colIndexMap) {
        List<String> tokens = tokenize(formula);
        List<String> rpn = toRPN(tokens);
        return evalRPN(rpn, row, colIndexMap);
    }

    /**
     * Splits a formula string into a flat list of tokens.
     *
     * <p>Characters that are letters, digits, {@code .}, or {@code _} are accumulated into
     * a single multi-character token (representing a column name or numeric literal).
     * Operator characters ({@code +}, {@code -}, {@code *}, {@code /}) and parentheses
     * are emitted as single-character tokens. Whitespace is silently skipped.
     *
     * <p>The support for {@code _} is critical for join-column names such as
     * {@code right_count_product_id} which would otherwise be split into multiple tokens.
     *
     * @param expr the formula string to tokenise
     * @return ordered list of tokens ready for shunting-yard processing
     */
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

    /**
     * Converts an infix token list to Reverse Polish Notation using Dijkstra's
     * shunting-yard algorithm.
     *
     * <p>Operator precedence: {@code *} and {@code /} bind more tightly than {@code +}
     * and {@code -}. Parentheses are consumed from the output and used only to control
     * the operator stack. The algorithm correctly handles left-associativity for all
     * four supported operators.
     *
     * @param tokens the infix token list produced by {@link #tokenize}
     * @return the equivalent RPN token list ready for stack-based evaluation
     */
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

    /**
     * Evaluates a Reverse Polish Notation token list against a data row.
     *
     * <p>Operand tokens (column names or numeric literals) are pushed onto a {@code double}
     * stack via {@link #getValue}. Operator tokens pop two operands, compute the result,
     * and push it back. The final stack top is the formula result.
     *
     * @param rpn         the RPN token list from {@link #toRPN}
     * @param row         the current data row
     * @param colIndexMap mapping of column name → index in {@code row}
     * @return the evaluated double result
     */
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

    /**
     * Returns {@code true} if the token is an operand (column name or numeric literal)
     * rather than an operator or parenthesis.
     *
     * @param token a token from the infix list
     * @return {@code true} for operand tokens, {@code false} for operators and parentheses
     */
    private boolean isNumberOrColumn(String token) {
        return !(token.equals("+") || token.equals("-") ||
                token.equals("*") || token.equals("/") ||
                token.equals("(") || token.equals(")"));
    }

    /**
     * Resolves a token to a {@code double} value.
     *
     * <p>First tries to find the token as a column name in {@code colIndexMap} and parse
     * the corresponding cell as a double. If the column is not found or the cell is not
     * numeric, falls back to parsing the token itself as a numeric literal. Returns 0.0
     * if both attempts fail.
     *
     * @param token       an operand token (column name or literal)
     * @param row         the current data row
     * @param colIndexMap mapping of column name → index
     * @return the resolved double value, or {@code 0.0} on parse failure
     */
    private double getValue(String token, String[] row, Map<String, Integer> colIndexMap) {
        Integer idx = colIndexMap.get(token);
        if (idx != null && idx < row.length) {
            try { return Double.parseDouble(row[idx]); } catch (Exception e) { return 0; }
        }
        try { return Double.parseDouble(token); } catch (Exception e) { return 0; }
    }
}
