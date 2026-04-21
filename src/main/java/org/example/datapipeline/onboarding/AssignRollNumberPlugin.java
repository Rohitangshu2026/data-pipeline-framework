package org.example.datapipeline.onboarding;

import org.example.datapipeline.executor.context.ExecutionContext;
import org.example.datapipeline.executor.iterator.DataIterator;
import org.example.datapipeline.plugin.ActionPlugin;
import org.example.datapipeline.plugin.Executor;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AssignRollNumberPlugin implements ActionPlugin {

    @Override
    public String getType() {
        return "assign_roll_number";
    }

    @Override
    public String getName() {
        return "assign_roll_number";
    }

    private final Map<String, AtomicInteger> deptCounters = new java.util.concurrent.ConcurrentHashMap<>();

    @Override
    public Executor getExecutor() {
        return ctx -> {
            DataIterator input = ctx.getIterator();
            Map<String, String> params = ctx.getMethod().getParamMap();
            
            String formatStr = params.get("format");
            if (formatStr == null || formatStr.isEmpty()) {
                throw new RuntimeException("Missing 'format' parameter for assign_roll_number");
            }
            String year = params.getOrDefault("year", "");
            boolean strictDeptMapping = "true".equalsIgnoreCase(params.get("strict_dept_mapping"));
            
            String deptMapStr = params.get("dept_code_map");
            Map<String, String> deptMap = new HashMap<>();
            if (deptMapStr != null && !deptMapStr.isEmpty()) {
                for (String mapping : deptMapStr.split(",")) {
                    String[] parts = mapping.split(":");
                    if (parts.length == 2) {
                        deptMap.put(parts[0].trim().toUpperCase(), parts[1].trim());
                    }
                }
            }

            return new DataIterator() {
                boolean headerProcessed = false;
                int deptColIndex = -1;
                int yearColIndex = -1;
                int countIn = 0;
                int countOut = 0;

                @Override
                public boolean hasNext() {
                    boolean has = input.hasNext();
                    if (!has && countIn > 0) {
                        System.out.println("AssignRollNumberPlugin: ROWS_IN = " + countIn + ", ROWS_OUT = " + countOut);
                    }
                    return has;
                }

                @Override
                public String[] next() {
                    countIn++;
                    if (!headerProcessed) {
                        String[] header = input.next();
                        for (int i = 0; i < header.length; i++) {
                            if (header[i].equalsIgnoreCase("department")) {
                                deptColIndex = i;
                            }
                            if (header[i].equalsIgnoreCase("year")) {
                                yearColIndex = i;
                            }
                        }
                        String[] newHeader = new String[header.length + 1];
                        System.arraycopy(header, 0, newHeader, 0, header.length);
                        newHeader[header.length] = "roll_number";
                        headerProcessed = true;
                        countOut++;
                        return newHeader;
                    }

                    String[] row = input.next();
                    String[] newRow = new String[row.length + 1];
                    System.arraycopy(row, 0, newRow, 0, row.length);

                    String deptRaw = deptColIndex != -1 && deptColIndex < row.length ? row[deptColIndex].trim() : "";
                    String deptKey = deptRaw.toUpperCase();
                    
                    String rowYear = yearColIndex != -1 && yearColIndex < row.length ? row[yearColIndex].trim() : year;
                    if (rowYear.isEmpty()) {
                        rowYear = year;
                    }

                    String deptCode;
                    if (deptMap.containsKey(deptKey)) {
                        deptCode = deptMap.get(deptKey);
                    } else {
                        if (strictDeptMapping) {
                            deptCode = "FAILED";
                        } else {
                            // Fallback: use first token uppercase
                            deptCode = deptKey.split("\\s+")[0];
                        }
                    }
                    
                    int count = deptCounters.computeIfAbsent(deptCode, k -> new AtomicInteger(0)).incrementAndGet();
                    System.out.println("AssignRollNumberPlugin: Assigning " + deptCode + " counter: " + count);
                    String rollNumber = deptCode.equals("FAILED") ? "FAILED" : resolveFormat(formatStr, rowYear, deptCode, count);
                    newRow[row.length] = rollNumber;

                    countOut++;
                    return newRow;
                }
                
                private String resolveFormat(String fmt, String y, String d, int c) {
                    String res = fmt.replace("{year}", y).replace("{dept_code}", d);
                    
                    Matcher m = Pattern.compile("\\{counter(?::(\\d+))?\\}").matcher(res);
                    if (m.find()) {
                        String padStr = m.group(1);
                        String counterStr;
                        if (padStr != null) {
                            int pad = Integer.parseInt(padStr);
                            counterStr = String.format("%0" + pad + "d", c);
                        } else {
                            counterStr = String.valueOf(c);
                        }
                        res = m.replaceAll(counterStr);
                    }
                    return res;
                }
            };
        };
    }
}
